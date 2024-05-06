from sqlmodel import Field, Relationship, SQLModel
from datetime import datetime
from typing import List, TYPE_CHECKING
import sqlalchemy as sa


# https://sqlmodel.tiangolo.com/tutorial/code-structure/
# pragma: no cover means should not be included for unit test coverage
if TYPE_CHECKING:  # pragma: no cover
    from .catalog import Table, Stage


class LoggingBase(SQLModel, table=False):
    # __table_args__ = {"schema": "logging"}
    pass


class StageLog(LoggingBase, table=True):
    __tablename__ = "stage_logs"

    id: int | None = Field(default=None, primary_key=True, index=True)

    # The relationship is defined onesided so that when a log is queried all table info is available.
    # But not all logs are selected every time a table is queried.
    table_id: int = Field(foreign_key="tables.id", index=True)
    stage_log_table: "Table" = Relationship(back_populates="")

    # The relationship is defined onesided so that when a log is queried all stage info is available.
    # But not all logs are selected every time a stage is queried.
    stage_id: int = Field(foreign_key="stages.id", index=True)
    stage_log_stage: "Stage" = Relationship(back_populates="")

    stage_log_stage_log_messages: List["StageLogMessage"] | None = Relationship(
        back_populates="stage_log_message_stage_log"
    )

    cdc_key: int = Field(index=True)
    datetime_started: datetime = Field(default_factory=datetime.now)
    is_open: bool = Field(default=True, index=True)
    run_id: str | None = Field(
        default=None,
        max_length=256,
        sa_type=sa.String(length=256),
        description="A unique identifier for a run if available, I.E.: adf_run_id",
    )

    datetime_ended: datetime | None = Field(default=None)
    success: bool | None = Field(default=None, index=True)
    number_of_records_processed: int | None = Field(default=None)

    class Open(LoggingBase, table=False):
        """
        Represents the data required to create a new stage log.
        """

        table_id: int
        stage_id: int
        cdc_key: int
        run_id: str | None = Field(max_length=256, default=None)

    class Close(LoggingBase, table=False):
        """
        Represents the data required to update an existing stage log.
        """

        id: int
        success: bool
        number_of_records_processed: int | None = Field(default=None)

    class Return(LoggingBase, table=False):
        """
        Represents the data returned when querying a stage log.
        """

        id: int
        table_id: int
        stage_id: int
        datetime_started: datetime
        is_open: bool
        cdc_key: int
        run_id: str | None

        # Log might not be closed so closing field can be None
        success: bool | None
        number_of_records_processed: int | None
        datetime_ended: datetime | None


class StageLogMessage(LoggingBase, table=True):
    __tablename__ = "stage_log_messages"

    id: int | None = Field(default=None, primary_key=True, index=True)

    stage_log_id: int = Field(foreign_key="stage_logs.id", index=True)
    stage_log_message_stage_log: List["StageLog"] = Relationship(
        back_populates="stage_log_stage_log_messages"
    )

    message: str = Field(max_length=1024, sa_column=sa.String(length=1024))
    is_error: bool
    datetime_stage_log_message: datetime = Field(default_factory=datetime.now)

    class Add(LoggingBase, table=False):
        stage_log_id: int
        message: str = Field(max_length=1024)
        is_error: bool

    class Return(LoggingBase, table=False):
        id: int
        stage_log_id: int
        message: str = Field(max_length=1024)
        is_error: bool
        datetime_stage_log_message: datetime
