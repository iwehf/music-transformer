from enum import Enum

import sqlalchemy as sa
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base, BaseMixin


class TaskStatus(Enum):
    Pending = "pending"
    Running = "running"
    Finished = "finished"


class Task(Base, BaseMixin):
    __tablename__ = "tasks"

    task_id: Mapped[int] = mapped_column(sa.Integer, index=True, nullable=False)
    creator: Mapped[str] = mapped_column(sa.String, index=False, nullable=False)
    worker_count: Mapped[int] = mapped_column(sa.Integer, index=False, nullable=False)
    max_round: Mapped[int] = mapped_column(sa.Integer, index=False, nullable=False)
    curr_round: Mapped[int] = mapped_column(
        sa.Integer, index=False, nullable=False, default=0
    )
    hf_repo_url: Mapped[str] = mapped_column(
        sa.String, index=False, nullable=False, default=""
    )
    status: Mapped[TaskStatus] = mapped_column(
        sa.Enum(TaskStatus), index=True, nullable=False, default=TaskStatus.Pending
    )


class TaskWorker(Base, BaseMixin):
    __tablename__ = "task_workers"

    task_id: Mapped[int] = mapped_column(sa.Integer, index=True, nullable=False)
    address: Mapped[str] = mapped_column(sa.String, index=False, nullable=False)
    num_samples: Mapped[int] = mapped_column(sa.Integer, index=False, nullable=False)


class TaskLogType(Enum):
    StartTask = "start_task"
    StartRound = "start_round"
    UploadData = "upload_data"
    AggregateData = "aggregate_data"
    FinishTask = "finish_task"


class TaskLog(Base, BaseMixin):
    __tablename__ = "task_logs"

    task_id: Mapped[int] = mapped_column(sa.Integer, index=True, nullable=False)
    type: Mapped[TaskLogType] = mapped_column(
        sa.Enum(TaskLogType), index=True, nullable=False
    )
    address: Mapped[str] = mapped_column(sa.String, index=False, nullable=False)
    tx_hash: Mapped[str] = mapped_column(sa.String, index=False, nullable=False)
    round: Mapped[int] = mapped_column(sa.Integer, index=False, nullable=False)


class TaskMetric(Base, BaseMixin):
    __tablename__ = "task_metrics"

    task_id: Mapped[int] = mapped_column(sa.Integer, index=True, nullable=False)
    round: Mapped[int] = mapped_column(sa.Integer, index=False, nullable=False)
    loss: Mapped[float] = mapped_column(sa.Double, index=False, nullable=False)
