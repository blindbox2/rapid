import pytest
from sqlmodel import Session
from ...crud.catalog import stage_crud
from ...models.catalog import Stage
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError


valid_stage = Stage.Create(name="1", description="1")


def test_create_stage(session: Session):
    created_stage = stage_crud.insert_into_table(session, valid_stage)

    assert created_stage.id == 1
    assert created_stage.name == "1"
    assert created_stage.description == "1"


def test_create_invalid_stage():
    with pytest.raises(ValidationError) as exception_info:
        Stage.Create(name=1, description=1)

    assert len(exception_info.value.errors()) == 2


def test_get_stage(session: Session):
    stage_crud.insert_into_table(session, valid_stage)
    db_stage = stage_crud.select_on_pk(session, 1)

    assert db_stage.id == 1
    assert db_stage.name == "1"
    assert db_stage.description == "1"


def test_get_stage_invalid(session: Session):
    with pytest.raises(ValueError) as exception_info:
        stage_crud.select_on_pk(session, model_id=1)

    assert str(exception_info.value) == "404: stage with ID: 1 not found."


def test_get_stages(session: Session):
    stage1 = Stage.Create(name="2", description="2")

    stage_crud.insert_into_table(session, valid_stage)
    stage_crud.insert_into_table(session, stage1)

    db_stages = stage_crud.select_all(session)
    assert len(db_stages) == 2
    assert db_stages[0].id == 1
    assert db_stages[1].id == 2


def test_get_stages_invalid(session: Session):
    with pytest.raises(ValueError) as exception_info:
        stage_crud.select_all(session)

    assert str(exception_info.value) == "404: no stages found."


def test_update_stage(session: Session):
    db_stage = stage_crud.insert_into_table(session, valid_stage)

    stage_update = Stage.Update(name="2", description="2")
    db_changed_stage = stage_crud.update_table_on_pk(session, db_stage.id, stage_update)

    assert db_changed_stage.name == "2"
    assert db_changed_stage.description == "2"


def test_update_invalid(session: Session):
    stage_crud.insert_into_table(session, valid_stage)

    with pytest.raises(ValidationError) as exception_info:
        Stage.Update(name=1, description=True)

    assert len(exception_info.value.errors()) == 2


def test_soft_delete_stage(session: Session):
    stage_crud.insert_into_table(session, valid_stage)
    db_stage = stage_crud.select_on_pk(session, model_id=1)
    assert db_stage.is_active

    stage_crud.delete_from_table(session, model_id=1)
    db_deleted_stage = stage_crud.select_on_pk(session, model_id=1)

    assert not db_deleted_stage.is_active


def test_hard_delete_stage(session: Session):
    stage_crud.insert_into_table(session, valid_stage)
    stage_crud.delete_from_table(session, model_id=1, hard_delete=True)

    with pytest.raises(ValueError) as exception_info:
        stage_crud.select_on_pk(session, 1)

    assert str(exception_info.value) == "404: stage with ID: 1 not found."


def test_delete_invalid(session):
    with pytest.raises(ValueError) as exception_info:
        stage_crud.delete_from_table(session, 1)

    assert str(exception_info.value) == "404: stage with ID: 1 not found."


def test_stage_unique(session):
    stage_crud.insert_into_table(session, valid_stage)
    with pytest.raises(IntegrityError) as _:
        stage_crud.insert_into_table(session, valid_stage)
