import json
import os
import threading
from queue import SimpleQueue
from tempfile import TemporaryFile
from typing import Optional

import requests
from websockets.sync.client import connect

from fl_task_contract import FLTask
from fl_utils import dump_data, load_data
from hparams import hparams
from model import MusicTransformer


class TrainWorker(threading.Thread):
    def __init__(self, server_url: str, contract: FLTask, num_samples: int) -> None:
        super().__init__(daemon=True)

        self.url = server_url
        self.contract = contract
        self.num_samples = num_samples

        self.start_event = threading.Event()
        self.stop_event = threading.Event()
        self.input_queue = SimpleQueue()
        self.output_queue = SimpleQueue()

        self.max_round: int = 0

    def wait_start(self, timeout: Optional[float] = None):
        self.start_event.wait(timeout=timeout)
        return self.max_round

    def send_data(self, data):
        self.input_queue.put({"type": "data", "data": data})

    def send_metric(self, metric):
        self.input_queue.put({"type": "metric", "data": metric})

    def get_agg_data(self):
        return self.output_queue.get()

    def stop(self):
        self.stop_event.set()

    def join_task(self, task_id: int) -> int:
        receipt = self.contract.join_task(task_id)
        events = self.contract.process_event("WorkerJoined", receipt)
        worker_id = events[0]["args"]["workerID"]
        return worker_id

    def upload_data(self, task_id: int, round: int, worker_id: int, data):
        with TemporaryFile() as f:
            dump_data(data, f)
            f.seek(0)
            with requests.post(
                f"{self.url}/task/data",
                data={"task_id": task_id, "round": round, "worker_id": worker_id},
                files={"file": f},
            ) as resp:
                resp.raise_for_status()
                status = resp.json()["status"]
                assert status == "success"
        return self.contract.upload_data(task_id, round, worker_id)

    def recv_agg_data(self, task_id: int, round: int):
        with requests.get(
            f"{self.url}/task/data",
            params={"task_id": task_id, "round": round},
            stream=True,
        ) as resp:
            resp.raise_for_status()
            with TemporaryFile() as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
                f.seek(0)
                data = load_data(f)
        return data

    def run(self):
        with connect(f"{self.url}/ws", max_size=None) as websocket:
            websocket.send(
                json.dumps(
                    {
                        "address": self.contract.acct.address,
                        "num_samples": self.num_samples,
                    }
                )
            )
            while not self.stop_event.is_set():
                self.start_event.clear()
                task_msg = websocket.recv()
                assert isinstance(task_msg, str)
                if task_msg == "no task":
                    continue
                task_info = json.loads(task_msg)
                assert "task_id" in task_info
                task_id = task_info["task_id"]
                max_round = task_info["max_round"]
                self.max_round = max_round
                print(f"task {task_id} max rounds {max_round}")

                worker_id = self.join_task(task_id)
                websocket.send(json.dumps({"task_id": task_id, "worker_id": worker_id}))
                print(f"task {task_id} worker id {worker_id}")

                round_msg = websocket.recv()
                curr_round = json.loads(round_msg)["round"]
                print(f"task {task_id} round {curr_round}")
                self.start_event.set()

                while curr_round <= max_round:
                    input = self.input_queue.get()
                    type = input["type"]
                    if type == "data":
                        data = input["data"]
                        request = {
                            "task_id": task_id,
                            "worker_id": worker_id,
                            "round": curr_round,
                            "type": "data",
                        }
                        websocket.send(json.dumps(request))
                        print(f"task {task_id} round {curr_round} upload data")
                        receipt = self.upload_data(task_id, curr_round, worker_id, data)
                        tx_hash = receipt["transactionHash"]
                        websocket.send(tx_hash)
                        print(f"task {task_id} round {curr_round} upload data tx hash")
                        msg = websocket.recv()
                        assert isinstance(msg, str)
                        assert msg == "data ready"
                        agg_data = self.recv_agg_data(task_id, curr_round)
                        print(
                            f"task {task_id} round {curr_round} receive aggregated data"
                        )
                        self.output_queue.put(agg_data)
                        curr_round += 1
                        if curr_round <= max_round:
                            round_msg = websocket.recv()
                            server_round = json.loads(round_msg)["round"]
                            assert curr_round == server_round
                            print(f"task {task_id} round {curr_round}")
                    elif type == "metric":
                        metric = input["data"]
                        request = {
                            "task_id": task_id,
                            "worker_id": worker_id,
                            "round": curr_round,
                            "type": "metric",
                        }
                        websocket.send(json.dumps(request))
                        websocket.send(json.dumps(metric))

                leave_request = {
                    "task_id": task_id,
                    "worker_id": worker_id,
                    "type": "leave",
                }
                websocket.send(json.dumps(leave_request))


def main():
    privkey = os.getenv("FL_PRIVKEY")
    assert privkey is not None
    contract_address = os.getenv("FL_CONTRACT_ADDRESS")
    assert contract_address is not None
    contract = FLTask(
        url="https://crynux-testnet.public.blastapi.io",
        privkey=privkey,
        contract_address=contract_address,
    )
    server_url = "ws://localhost:8000"
    client = TrainWorker(server_url=server_url, contract=contract, num_samples=100)
    client.start()

    m = MusicTransformer(**hparams)

    while True:
        try:
            max_round = client.wait_start()
            for _ in range(max_round):
                data = {name: p.detach() for name, p in m.named_parameters()}
                client.send_data(data)
                client.get_agg_data()

                metric = {"count": 100, "loss": 0.1}
                client.send_metric(metric)
        except KeyboardInterrupt:
            break
    client.stop()
    client.join()


if __name__ == "__main__":
    main()
