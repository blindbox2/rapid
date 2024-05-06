import pytest
from sqlmodel import Session
from ...models.rapid_logging import StageLog, StageLogMessage

from ...crud.rapid_logging import stage_log_crud, stage_log_message_crud
from pydantic import ValidationError
from datetime import datetime


valid_stage_log_message = StageLogMessage.Add(
    stage_log_id=1, message="test", is_error=True
)


def test_add_stage_log_message(session: Session):
    stage_log = StageLog.Open(table_id=1, stage_id=1, cdc_key=1)
    _ = stage_log_crud.open_stage_log(session=session, stage_log=stage_log)

    added_stage_log_message = stage_log_message_crud.add_stage_log_message(
        session, valid_stage_log_message
    )

    assert added_stage_log_message.id == 1
    assert added_stage_log_message.stage_log_id == 1
    assert added_stage_log_message.message == "test"
    assert added_stage_log_message.is_error
    assert isinstance(added_stage_log_message.datetime_stage_log_message, datetime)


def test_add_stage_log_message_invalid():
    with pytest.raises(ValidationError) as exception_info:
        _ = StageLogMessage.Add(stage_log_id="a", message=1, is_error="a")

    assert len(exception_info.value.errors()) == 3


def test_get_stage_log(session: Session):
    stage_log = StageLog.Open(table_id=1, stage_id=1, cdc_key=1)
    _ = stage_log_crud.open_stage_log(session=session, stage_log=stage_log)

    added_stage_log_message = stage_log_message_crud.add_stage_log_message(
        session, valid_stage_log_message
    )

    db_stage_log_message = stage_log_message_crud.get_stage_log_message_on_id(
        session, added_stage_log_message.id
    )

    assert db_stage_log_message.id == 1
    assert db_stage_log_message.stage_log_id == 1
    assert db_stage_log_message.message == "test"
    assert db_stage_log_message.is_error
    assert isinstance(db_stage_log_message.datetime_stage_log_message, datetime)


def test_get_stage_log_message_invalid(session: Session):
    with pytest.raises(ValueError) as exception_info:
        stage_log_message_crud.get_stage_log_message_on_id(
            session, stage_log_message_id=1
        )

    assert str(exception_info.value) == "404: stage_log_message with ID: 1 not found."


def test_delete_stage_log_message(session: Session):
    stage_log = StageLog.Open(table_id=1, stage_id=1, cdc_key=1)
    _ = stage_log_crud.open_stage_log(session=session, stage_log=stage_log)

    db_stage_log_message = stage_log_message_crud.add_stage_log_message(
        session=session, stage_log_message=valid_stage_log_message
    )

    stage_log_message_crud.delete_stage_log_message(
        session, stage_log_message_id=db_stage_log_message.id
    )

    with pytest.raises(ValueError) as exception_info:
        stage_log_message_crud.get_stage_log_message_on_id(
            session, stage_log_message_id=db_stage_log_message.id
        )

    assert str(exception_info.value) == "404: stage_log_message with ID: 1 not found."


def test_delete_stage_log_invalid(session: Session):
    with pytest.raises(ValueError) as exception_info:
        stage_log_message_crud.delete_stage_log_message(
            session=session, stage_log_message_id=1
        )

    assert str(exception_info.value) == "404: stage_log_message with ID: 1 not found."


def test_adding_stage_log_message_to_closed_stage_log(session: Session):
    stage_log_open = StageLog.Open(table_id=1, stage_id=1, cdc_key=1)
    stage_log_crud.open_stage_log(session=session, stage_log=stage_log_open)

    stage_log_close = StageLog.Close(id=1, success=True, number_of_records_processed=1)
    stage_log_crud.close_stage_log(session, stage_log_close)

    with pytest.raises(ValueError) as exception_info:
        stage_log_message_crud.add_stage_log_message(session, valid_stage_log_message)

    assert (
        str(exception_info.value)
        == "403: Forbidden to add stage_log_messages to closed stage_log with ID: 1."
    )
