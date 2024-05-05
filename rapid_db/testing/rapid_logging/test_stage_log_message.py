import pytest
from sqlmodel import Session, SQLModel, create_engine
from ...models.rapid_logging import StageLogMessage

from ...crud.rapid_logging import stage_log_message_crud
from pydantic import ValidationError
from datetime import datetime


@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine("sqlite://")
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


valid_stage_log_message = StageLogMessage.Add(
    stage_log_id=1, message="test", is_error=True
)


def test_add_stage_log_message(session: Session):
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
