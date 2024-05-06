import pytest
from sqlmodel import Session
from ...models.rapid_logging import StageLog

from ...crud.rapid_logging import stage_log_crud
from pydantic import ValidationError
from datetime import datetime


valid_stage_log = StageLog.Open(table_id=1, stage_id=1, cdc_key=1)


def test_open_stage_log(session: Session):
    opened_stage_log = stage_log_crud.open_stage_log(session, valid_stage_log)

    assert opened_stage_log.id == 1
    assert opened_stage_log.table_id == 1
    assert opened_stage_log.stage_id == 1
    assert opened_stage_log.cdc_key == 1
    assert opened_stage_log.is_open
    assert opened_stage_log.datetime_started is not None
    assert isinstance(opened_stage_log.datetime_started, datetime)
    assert opened_stage_log.run_id is None
    assert opened_stage_log.success is None
    assert opened_stage_log.number_of_records_processed is None
    assert opened_stage_log.datetime_ended is None


def test_open_stage_log_invalid():
    with pytest.raises(ValidationError) as exception_info:
        _ = StageLog.Open(table_id="a", stage_id="a", cdc_key="a", run_id=1)

    assert len(exception_info.value.errors()) == 4


def test_get_stage_log(session: Session):
    _ = stage_log_crud.open_stage_log(session, valid_stage_log)

    db_stage_log = stage_log_crud.get_stage_log_on_id(session, 1)

    assert db_stage_log.id == 1
    assert db_stage_log.table_id == 1
    assert db_stage_log.stage_id == 1
    assert db_stage_log.cdc_key == 1
    assert db_stage_log.is_open
    assert db_stage_log.datetime_started is not None
    assert isinstance(db_stage_log.datetime_started, datetime)
    assert db_stage_log.run_id is None
    assert db_stage_log.success is None
    assert db_stage_log.number_of_records_processed is None
    assert db_stage_log.datetime_ended is None


def test_get_stage_log_invalid(session: Session):
    with pytest.raises(ValueError) as exception_info:
        stage_log_crud.get_stage_log_on_id(session, stage_log_id=1)

    assert str(exception_info.value) == "404: stage_log with ID: 1 not found."


def test_close_stage_log(session: Session):
    db_stage_log = stage_log_crud.open_stage_log(session, valid_stage_log)

    stage_log_close = StageLog.Close(
        id=db_stage_log.id, success=True, number_of_records_processed=1
    )
    db_stage_log_close = stage_log_crud.close_stage_log(session, stage_log_close)

    assert db_stage_log_close.id == 1
    assert db_stage_log_close.table_id == 1
    assert db_stage_log_close.stage_id == 1
    assert db_stage_log_close.cdc_key == 1
    assert not db_stage_log_close.is_open
    assert isinstance(db_stage_log_close.datetime_started, datetime)
    assert db_stage_log_close.run_id is None
    assert db_stage_log_close.success
    assert db_stage_log_close.number_of_records_processed == 1
    assert isinstance(db_stage_log_close.datetime_ended, datetime)


def test_delete_stage_log(session: Session):
    _ = stage_log_crud.open_stage_log(session, valid_stage_log)

    stage_log_crud.delete_stage_log(session, stage_log_id=1)

    with pytest.raises(ValueError) as exception_info:
        stage_log_crud.get_stage_log_on_id(session, stage_log_id=1)

    assert str(exception_info.value) == "404: stage_log with ID: 1 not found."


def test_delete_stage_log_invalid(session: Session):
    with pytest.raises(ValueError) as exception_info:
        stage_log_crud.delete_stage_log(session, stage_log_id=1)

    assert str(exception_info.value) == "404: stage_log with ID: 1 not found."
