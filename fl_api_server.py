import os
from contextlib import asynccontextmanager
from typing import Annotated, List, Literal, Optional

import sqlalchemy as sa
import uvicorn
from fastapi import Depends, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

import db

db_filename = os.getenv("FL_DB", "fl_server.db")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init(db_filename)
    yield
    await db.close()


app = FastAPI(lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class WorkerRequest(BaseModel):
    task_id: int
    worker_id: int
    type: Literal["data", "metric", "leave"]
    round: Optional[int] = None


class CreateTaskInput(BaseModel):
    worker_count: int
    max_round: int


class CreateTaskResp(BaseModel):
    status: Literal["success", "error"] = "success"


@app.post("/task", response_model=CreateTaskResp)
async def create_task(
    input: CreateTaskInput,
    *,
    sess: Annotated[AsyncSession, Depends(db.get_session)],
):
    task = db.models.Task(
        task_id=0,
        creator="",
        worker_count=input.worker_count,
        max_round=input.max_round,
        status=db.models.TaskStatus.Pending
    )
    sess.add(task)
    await sess.commit()

    return CreateTaskResp()


class GetTaskResp(BaseModel):
    task_id: int
    current_round: int
    max_round: int
    latest_partial_time: int
    latest_aggregation_time: int
    hf_repo_url: str
    num_samples: int


@app.get("/task", response_model=GetTaskResp)
async def get_task(
    task_id: Optional[int] = None,
    *,
    sess: Annotated[AsyncSession, Depends(db.get_session)],
):
    q = sa.select(db.models.Task)
    if task_id is not None:
        q = q.where(db.models.Task.task_id == task_id)
    else:
        q = q.order_by(db.models.Task.task_id.desc()).limit(1)

    task = (await sess.execute(q)).scalar_one_or_none()
    if task is None:
        raise HTTPException(400, "No task")

    q = sa.select(db.models.TaskWorker).where(
        db.models.TaskWorker.task_id == task.task_id
    )
    workers = (await sess.execute(q)).scalars().all()
    total_num_samples = sum(worker.num_samples for worker in workers)

    q = (
        sa.select(db.models.TaskLog)
        .where(db.models.TaskLog.task_id == task.task_id)
        .where(db.models.TaskLog.type == db.models.TaskLogType.UploadData)
        .order_by(db.models.TaskLog.id.desc())
        .limit(1)
    )
    latest_partial_log = (await sess.execute(q)).scalar_one_or_none()
    if latest_partial_log is not None:
        latest_partial_time = int(latest_partial_log.created_at.timestamp())
    else:
        latest_partial_time = int(task.created_at.timestamp())

    q = (
        sa.select(db.models.TaskLog)
        .where(db.models.TaskLog.task_id == task.task_id)
        .where(db.models.TaskLog.type == db.models.TaskLogType.AggregateData)
        .order_by(db.models.TaskLog.id.desc())
        .limit(1)
    )
    latest_aggregation_log = (await sess.execute(q)).scalar_one_or_none()
    if latest_aggregation_log is not None:
        latest_aggregation_time = int(latest_aggregation_log.created_at.timestamp())
    else:
        latest_aggregation_time = int(task.created_at.timestamp())

    return GetTaskResp(
        task_id=task.task_id,
        current_round=task.curr_round,
        max_round=task.max_round,
        latest_partial_time=latest_partial_time,
        latest_aggregation_time=latest_aggregation_time,
        hf_repo_url=task.hf_repo_url,
        num_samples=total_num_samples,
    )


class TaskNode(BaseModel):
    address: str
    contribution: float
    last_commit: int


class GetTaskNodesResp(BaseModel):
    nodes: List[TaskNode]


@app.get("/task/nodes", response_model=GetTaskNodesResp)
async def get_task_nodes(
    task_id: Optional[int] = None,
    *,
    sess: Annotated[AsyncSession, Depends(db.get_session)],
):
    q = sa.select(db.models.Task)
    if task_id is not None:
        q = q.where(db.models.Task.task_id == task_id)
    else:
        q = q.order_by(db.models.Task.task_id.desc()).limit(1)

    task = (await sess.execute(q)).scalar_one_or_none()
    if task is None:
        raise HTTPException(400, "No task")

    q = sa.select(db.models.TaskWorker).where(
        db.models.TaskWorker.task_id == task.task_id
    )
    workers = (await sess.execute(q)).scalars().all()
    total_num_samples = sum(worker.num_samples for worker in workers)
    addresses = [worker.address for worker in workers]

    q = (
        sa.select(
            db.models.TaskLog.address,
            sa.func.max(db.models.TaskLog.created_at).label("time"),
        )
        .where(db.models.TaskLog.task_id == task.task_id)
        .where(db.models.TaskLog.address.in_(addresses))
        .group_by(db.models.TaskLog.address)
    )
    res = (await sess.execute(q)).all()
    node_last_commits = {}
    for row in res:
        node_last_commits[row[0]] = int(row[1].timestamp())

    task_nodes = []
    for worker in workers:
        task_nodes.append(
            TaskNode(
                address=worker.address,
                contribution=worker.num_samples / total_num_samples,
                last_commit=node_last_commits[worker.address],
            )
        )

    return GetTaskNodesResp(nodes=task_nodes)


class TaskLog(BaseModel):
    type: Literal["submit", "aggregation"]
    time: int
    address: str
    tx_hash: str
    iteration: int


class GetTaskLogsResp(BaseModel):
    logs: List[TaskLog]


@app.get("/task/logs", response_model=GetTaskLogsResp)
async def get_task_logs(
    task_id: Optional[int] = None,
    page: int = 1,
    page_size: int = 500,
    *,
    sess: Annotated[AsyncSession, Depends(db.get_session)],
):
    q = sa.select(db.models.Task)
    if task_id is not None:
        q = q.where(db.models.Task.task_id == task_id)
    else:
        q = q.order_by(db.models.Task.task_id.desc()).limit(1)

    task = (await sess.execute(q)).scalar_one_or_none()
    if task is None:
        raise HTTPException(400, "No task")

    limit = page_size
    offset = (page - 1) * page_size
    q = (
        sa.select(db.models.TaskLog)
        .where(
            db.models.TaskLog.task_id == task.task_id,
            sa.or_(
                db.models.TaskLog.type == db.models.TaskLogType.UploadData,
                db.models.TaskLog.type == db.models.TaskLogType.AggregateData,
            ),
        )
        .order_by(db.models.TaskLog.id.desc())
        .offset(offset)
        .limit(limit)
    )
    logs = (await sess.execute(q)).scalars().all()
    res: List[TaskLog] = []
    for log in logs:
        if log.type == db.models.TaskLogType.UploadData:
            log_type = "submit"
        else:
            log_type = "aggregation"
        res.append(
            TaskLog(
                type=log_type,
                time=int(log.created_at.timestamp()),
                address=log.address,
                tx_hash=log.tx_hash,
                iteration=log.round,
            )
        )
    return GetTaskLogsResp(logs=res)


class TaskMetric(BaseModel):
    loss: float
    iteration: int


class GetTaskMetricsResp(BaseModel):
    metrics: List[TaskMetric]


@app.get("/task/metrics", response_model=GetTaskMetricsResp)
async def get_task_metrics(
    task_id: Optional[int] = None,
    page: int = 1,
    page_size: int = 500,
    *,
    sess: Annotated[AsyncSession, Depends(db.get_session)],
):
    q = sa.select(db.models.Task)
    if task_id is not None:
        q = q.where(db.models.Task.task_id == task_id)
    else:
        q = q.order_by(db.models.Task.task_id.desc()).limit(1)

    task = (await sess.execute(q)).scalar_one_or_none()
    if task is None:
        raise HTTPException(400, "No task")

    limit = page_size
    offset = (page - 1) * page_size
    q = (
        sa.select(db.models.TaskMetric)
        .where(db.models.TaskMetric.task_id == task.task_id)
        .order_by(db.models.TaskMetric.id)
        .offset(offset)
        .limit(limit)
    )
    task_metrics = (await sess.execute(q)).scalars().all()
    metrics: List[TaskMetric] = []
    for metric in task_metrics:
        metrics.append(TaskMetric(loss=metric.loss, iteration=metric.round))
    return GetTaskMetricsResp(metrics=metrics)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
