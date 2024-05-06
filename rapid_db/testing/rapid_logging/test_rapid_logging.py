from sqlmodel import Session

from ...models.rapid_logging import StageLog, StageLogMessage
from ...crud.rapid_logging import stage_log_crud, stage_log_message_crud

from ...models.catalog import Table, Stage
from ...crud.catalog import table_crud, stage_crud


def test_stage_log_table_relation(session: Session):
    table = Table.Create(
        name="test table",
        description="test table description",
        source_location="test source location",
        stage_id=1,
        source_id=1,
    )
    db_table = table_crud.insert_into_table(session=session, model=table)

    stage_log = StageLog.Open(table_id=db_table.id, stage_id=1, cdc_key=1)
    db_stage_log = stage_log_crud.open_stage_log(session=session, stage_log=stage_log)

    # Check that the stage_log has a related table
    assert db_stage_log.stage_log_table.id == db_table.id
    assert db_stage_log.table_id == db_table.id

    # Check that the relation is correctly set up
    assert db_table is db_stage_log.stage_log_table


def test_stage_log_stage_relation(session: Session):
    stage = Stage.Create(name="test stage", description="test stage description")
    db_stage = stage_crud.insert_into_table(session=session, model=stage)

    stage_log = StageLog.Open(table_id=1, stage_id=db_stage.id, cdc_key=1)
    db_stage_log = stage_log_crud.open_stage_log(session=session, stage_log=stage_log)

    # Check that the stage_log has a related stage
    assert db_stage_log.stage_log_stage.id == db_stage.id
    assert db_stage_log.stage_id == db_stage.id

    # Check that the relation is correctly set up
    assert db_stage is db_stage_log.stage_log_stage


def test_stage_log_stage_log_message_relation(session: Session):
    stage_log = StageLog.Open(table_id=1, stage_id=1, cdc_key=1)
    db_stage_log = stage_log_crud.open_stage_log(session=session, stage_log=stage_log)

    stage_log_message = StageLogMessage.Add(
        stage_log_id=1, message="test", is_error=True
    )
    stage_log_message1 = StageLogMessage.Add(
        stage_log_id=1, message="test1", is_error=True
    )

    db_stage_log_message = stage_log_message_crud.add_stage_log_message(
        session, stage_log_message
    )
    _ = stage_log_message_crud.add_stage_log_message(session, stage_log_message1)

    # Check that the stage_log has related stage_log_messages
    assert len(db_stage_log.stage_log_stage_log_messages) == 2
    assert db_stage_log.stage_log_stage_log_messages[0].id == db_stage_log_message.id

    # Check that the stage_log_message has a related stage_log
    assert db_stage_log_message.stage_log_id == db_stage_log.id
    assert db_stage_log_message.stage_log_message_stage_log.id == db_stage_log.id

    # Check that the relation is correctly set up in both directions
    assert db_stage_log_message in db_stage_log.stage_log_stage_log_messages
    assert db_stage_log is db_stage_log_message.stage_log_message_stage_log
