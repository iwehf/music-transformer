import asyncio
import os
import time
from typing import Annotated, Any, Dict, List, Literal, Optional

import sqlalchemy as sa
import uvicorn
from fastapi import Depends, FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

import db
from fl_task_contract import AsyncFLTask
from fl_utils import dump_data, load_data

db_filename = os.getenv("FL_DB", "fl_server.db")


app = FastAPI()


class WorkerRequest(BaseModel):
    task_id: int
    worker_id: int
    type: Literal["data", "metric", "leave"]
    round: Optional[int] = None


class TaskManager(object):
    def __init__(
        self,
        contract: AsyncFLTask,
        task_id: int,
        worker_count: int = 3,
        max_round: int = 100,
        wait_time: float = 60,
    ) -> None:
        self.contract = contract
        self.task_id = task_id
        self.worker_count = worker_count
        self.max_round = max_round
        self.wait_time = wait_time

        self._worker_condition = asyncio.Condition()
        self._workers: Dict[int, WebSocket] = {}

        self._start_event = asyncio.Event()

        self._round = 0

        self._data_condition = asyncio.Condition()
        self._data_pool: Dict[int, bytes] = {}

        self._metric_condition = asyncio.Condition()
        self._metric_pool: Dict[int, Dict[str, Any]] = {}

        self._recv_data_time = 0
        self._agg_data_time = 0
        self._send_data_time = 0

    async def close_all(self, reason: str = ""):
        futs = []
        for worker in self._workers.values():
            fut = asyncio.create_task(worker.close(reason=reason))
            futs.append(fut)

        await asyncio.gather(*futs)

    async def join(self, worker_id: int, websocket: WebSocket):
        async with self._worker_condition:
            self._workers[worker_id] = websocket
            self._worker_condition.notify()

    async def leave(self, worker_id: int):
        async with self._worker_condition:
            del self._workers[worker_id]
            self._worker_condition.notify()

    async def wait_worker_join(self):
        async with self._worker_condition:
            while len(self._workers) < self.worker_count:
                await self._worker_condition.wait()

            self._start_event.set()

    async def wait_task_start(self):
        await self._start_event.wait()

    async def wait_worker_leave(self):
        async with self._worker_condition:
            while len(self._workers) > 0:
                await self._worker_condition.wait()

    async def recv_data(self, worker_id: int, data_bytes: bytes):
        async with self._data_condition:
            assert worker_id not in self._data_pool
            self._data_pool[worker_id] = data_bytes
            self._data_condition.notify()

    async def agg_data(self):
        await self.start_round(1)
        while True:
            async with self._data_condition:
                while len(self._data_pool) < self.worker_count:
                    await self._data_condition.wait()

                round = self._round

                t0 = time.time()
                global_data = None
                for data_bytes in self._data_pool.values():
                    data = load_data(data_bytes)
                    if global_data is None:
                        global_data = data
                    else:
                        for k in data:
                            assert k in global_data
                            global_data[k].add_(data[k])

                assert global_data is not None
                for k in global_data:
                    global_data[k].div_(self.worker_count)

                self._data_pool.clear()
                self._agg_data_time += time.time() - t0

            t0 = time.time()
            data_bytes = dump_data(global_data)
            for websocket in self._workers.values():
                await websocket.send_bytes(data_bytes)

            self._send_data_time += time.time() - t0

            print(f"recv data time: {self._recv_data_time}")
            print(f"agg data time: {self._agg_data_time}")
            print(f"send data time: {self._send_data_time}")
            self._recv_data_time = 0
            self._agg_data_time = 0
            self._send_data_time = 0
            print(f"broadcast aggregated data of round {round} to workers")
            await self.aggregate_data(round)
            if round + 1 <= self.max_round:
                await self.start_round(round + 1)
            else:
                break

    async def recv_metric(self, worker_id: int, metric: Dict[str, Any]):
        async with self._metric_condition:
            assert worker_id not in self._metric_pool
            self._metric_pool[worker_id] = metric
            self._metric_condition.notify()

    async def agg_metric(self):
        while True:
            async with self._metric_condition:
                while len(self._metric_pool) < self.worker_count:
                    await self._metric_condition.wait()

                round = self._round
                global_metric = None
                for metric in self._metric_pool.values():
                    if global_metric is None:
                        global_metric = metric
                    else:
                        for k in metric:
                            assert k in global_metric
                            global_metric[k] += metric[k]

                self._metric_pool.clear()

            assert global_metric is not None
            loss = global_metric["loss"]
            count = global_metric["count"]
            avg_loss = loss / count
            async with db.session_scope() as sess:
                metric = db.models.TaskMetric(
                    task_id=self.task_id,
                    round=round,
                    loss=avg_loss,
                )
                sess.add(metric)
                await sess.commit()
            print(f"loss of round {round}: {avg_loss}")

    async def process_data_request(self, request: WorkerRequest, websocket: WebSocket):
        assert request.type == "data"
        assert self._round == request.round
        data_bytes = await websocket.receive_bytes()
        return data_bytes

    async def record_upload_data_log(self, address: str, websocket: WebSocket):
        tx_hash = await websocket.receive_bytes()
        async with db.session_scope() as sess:
            task_log = db.models.TaskLog(
                task_id=self.task_id,
                type=db.models.TaskLogType.UploadData,
                address=address,
                tx_hash="0x" + tx_hash.hex(),
                round=self._round,
            )
            sess.add(task_log)
            await sess.commit()

    async def process_metric_request(
        self, request: WorkerRequest, websocket: WebSocket
    ):
        assert request.type == "metric"
        assert self._round == request.round
        metric = await websocket.receive_json()
        return metric

    async def start_task(self):
        receipt = await self.contract.start_task(self.task_id)
        tx_hash = receipt["transactionHash"]
        async with db.session_scope() as sess:
            task_log = db.models.TaskLog(
                task_id=self.task_id,
                type=db.models.TaskLogType.StartTask,
                address=self.contract.acct.address,
                tx_hash=tx_hash.to_0x_hex(),
                round=1,
            )
            sess.add(task_log)
            await sess.commit()

    async def start_round(self, round: int):
        self._round = round
        receipt = await self.contract.start_round(self.task_id, round)
        tx_hash = receipt["transactionHash"]
        async with db.session_scope() as sess:
            task_log = db.models.TaskLog(
                task_id=self.task_id,
                type=db.models.TaskLogType.StartRound,
                address=self.contract.acct.address,
                tx_hash=tx_hash.to_0x_hex(),
                round=round,
            )
            sess.add(task_log)

            q = sa.select(db.models.Task).where(db.models.Task.task_id == self.task_id)
            task = (await sess.execute(q)).scalar_one()
            task.curr_round = round
            await sess.commit()
        print(f"task {self.task_id} start round {round}")
        for worker_id, websocket in self._workers.items():
            await websocket.send_json({"round": round})
            print(f"task {self.task_id} worker {worker_id} round {round}")

    async def aggregate_data(self, round: int):
        receipt = await self.contract.aggregate_data(self.task_id, round)
        tx_hash = receipt["transactionHash"]
        async with db.session_scope() as sess:
            task_log = db.models.TaskLog(
                task_id=self.task_id,
                type=db.models.TaskLogType.AggregateData,
                address=self.contract.acct.address,
                tx_hash=tx_hash.to_0x_hex(),
                round=round,
            )
            sess.add(task_log)
            await sess.commit()

    async def finish_task(self):
        receipt = await contract.finish_task(self.task_id)
        tx_hash = receipt["transactionHash"]
        async with db.session_scope() as sess:
            task_log = db.models.TaskLog(
                task_id=self.task_id,
                type=db.models.TaskLogType.FinishTask,
                address=self.contract.acct.address,
                tx_hash=tx_hash.to_0x_hex(),
                round=self._round,
            )
            sess.add(task_log)
            q = sa.select(db.models.Task).where(db.models.Task.task_id == self.task_id)
            task = (await sess.execute(q)).scalar_one()
            task.status = db.models.TaskStatus.Finished
            await sess.commit()

    async def serve_worker(self, address: str, websocket: WebSocket):
        await websocket.send_json(
            {"task_id": self.task_id, "max_round": self.max_round}
        )

        worker_id_msg = await websocket.receive_json()
        worker_id = worker_id_msg["worker_id"]
        await self.join(worker_id, websocket)
        print(f"worker {worker_id} has joined")

        await self.wait_task_start()
        print(f"task {self.task_id} starts")
        try:
            while True:
                request_msg = await websocket.receive_json()
                request = WorkerRequest.model_validate(request_msg)
                assert request.task_id == self.task_id
                assert request.worker_id == worker_id
                if request.type == "data":
                    t0 = time.time()
                    payload = await self.process_data_request(request, websocket)
                    await asyncio.gather(
                        self.record_upload_data_log(address, websocket),
                        self.recv_data(worker_id, payload),
                    )
                    self._recv_data_time += time.time() - t0
                    print(f"worker {worker_id} upload data of round {request.round}")
                elif request.type == "metric":
                    metric = await self.process_metric_request(request, websocket)
                    await self.recv_metric(worker_id, metric)
                    print(f"worker {worker_id} upload metric of round {request.round}")
                elif request.type == "leave":
                    print(f"worker {worker_id} send leave msg of task {self.task_id}")
                    break
        finally:
            await self.leave(worker_id)
            print(f"worker {worker_id} has left from task {self.task_id}")


privkey = os.getenv("FL_PRIVKEY")
assert privkey is not None
contract_address = os.getenv("FL_CONTRACT_ADDRESS")
assert contract_address is not None
blockchain_url = os.getenv("FL_BLOCKCHAIN_URL")
assert blockchain_url is not None

contract = AsyncFLTask(
    url=blockchain_url,
    privkey=privkey,
    contract_address=contract_address,
)
task_managers: Dict[int, TaskManager] = {}
pending_tasks = asyncio.Queue()


class CreateTaskInput(BaseModel):
    worker_count: int
    max_round: int


class CreateTaskResp(BaseModel):
    task_id: int


async def run_pending_tasks():
    running_tasks: List[asyncio.Task] = []

    try:
        while True:
            task_ids = []
            async with db.session_scope() as sess:
                q = (
                    sa.select(db.models.Task)
                    .where(db.models.Task.status == db.models.TaskStatus.Pending)
                    .order_by(db.models.Task.id)
                )
                tasks = (await sess.execute(q)).scalars().all()
                for task in tasks:
                    receipt = await contract.create_task(
                        task.worker_count, task.max_round
                    )
                    events = contract.process_event("TaskCreated", receipt)
                    assert len(events) == 1
                    task_id = events[0]["args"]["taskID"]
                    task_ids.append(task_id)
                    task_manager = TaskManager(
                        contract=contract,
                        task_id=task_id,
                        worker_count=task.worker_count,
                        max_round=task.max_round,
                    )
                    task_managers[task_id] = task_manager
                    for _ in range(task.worker_count):
                        await pending_tasks.put(task_id)
                    task.task_id = task_id
                    task.creator = contract.acct.address
                    task.status = db.models.TaskStatus.Running
                await sess.commit()

            for task_id in task_ids:
                task_manager = task_managers[task_id]
                fut = asyncio.create_task(run_task(task_manager))
                running_tasks.append(fut)

            await asyncio.sleep(1)
    except Exception as e:
        print(e)
    finally:
        for fut in running_tasks:
            fut.cancel()


async def run_task(task_manager: TaskManager):
    try:
        await asyncio.wait_for(task_manager.wait_worker_join(), task_manager.wait_time)
    except asyncio.TimeoutError:
        print(f"Not enough workers joined in {task_manager.wait_time} seconds")
        await task_manager.close_all()
        return

    await task_manager.start_task()

    t0 = time.time()
    data_task = asyncio.create_task(task_manager.agg_data())
    metric_task = asyncio.create_task(task_manager.agg_metric())

    try:
        await task_manager.wait_worker_leave()
        await task_manager.finish_task()
        del task_managers[task_manager.task_id]
    finally:
        data_task.cancel()
        metric_task.cancel()
        t1 = time.time()
        print(f"total time: {t1 - t0}")


@app.websocket("/ws")
async def handle_client(
    websocket: WebSocket,
    *,
    sess: Annotated[AsyncSession, Depends(db.get_session)],
):
    await websocket.accept()
    init_msg = await websocket.receive_json()
    assert "address" in init_msg
    assert "num_samples" in init_msg
    address = init_msg["address"]
    num_samples = init_msg["num_samples"]

    try:
        while True:
            try:
                task_id = pending_tasks.get_nowait()
                task_manager = task_managers[task_id]

                task_worker = db.models.TaskWorker(
                    task_id=task_id, address=address, num_samples=num_samples
                )
                sess.add(task_worker)
                await sess.commit()
                await task_manager.serve_worker(address, websocket)
            except asyncio.QueueEmpty:
                await websocket.send_text("no task")
                await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        pass
    finally:
        print("worker leave")


async def main():
    config = uvicorn.Config(app, host="127.0.0.1", port=8001)
    server = uvicorn.Server(config)

    await db.init(db_filename)
    try:
        task_fut = asyncio.create_task(run_pending_tasks())
        server_fut = asyncio.create_task(server.serve())
        await asyncio.gather(
            task_fut, server_fut
        )
    finally:
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())

