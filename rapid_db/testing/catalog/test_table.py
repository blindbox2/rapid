import pytest
from sqlmodel import Session
from ...crud.catalog import table_crud
from ...models.catalog import Table
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError


valid_table = Table.Create(
    name="1", description="1", source_location="1", stage_id=1, source_id=1
)


def test_create_table(session: Session):
    created_table = table_crud.insert_into_table(session, valid_table)

    assert created_table.id == 1
    assert created_table.name == "1"
    assert created_table.description == "1"
    assert created_table.source_location == "1"
    assert created_table.stage_id == 1
    assert created_table.source_id == 1


def test_create_invalid_table():
    with pytest.raises(ValidationError) as exception_info:
        Table.Create(
            name=1, description=1, source_location=1, stage_id=[], source_id={}
        )

    assert len(exception_info.value.errors()) == 5


def test_get_table(session: Session):
    table_crud.insert_into_table(session, valid_table)
    db_table = table_crud.select_on_pk(session, 1)

    assert db_table.id == 1
    assert db_table.name == "1"
    assert db_table.description == "1"
    assert db_table.source_location == "1"


def test_get_table_invalid(session: Session):
    with pytest.raises(ValueError) as exception_info:
        table_crud.select_on_pk(session, model_id=1)

    assert str(exception_info.value) == "404: table with ID: 1 not found."


def test_get_tables(session: Session):
    table1 = Table.Create(
        name="2", description="2", source_location="2", source_id=2, stage_id=2
    )

    table_crud.insert_into_table(session, valid_table)
    table_crud.insert_into_table(session, table1)

    db_tables = table_crud.select_all(session)
    assert len(db_tables) == 2
    assert db_tables[0].id == 1
    assert db_tables[1].id == 2


def test_get_tables_invalid(session: Session):
    with pytest.raises(ValueError) as exception_info:
        table_crud.select_all(session)

    assert str(exception_info.value) == "404: no tables found."


def test_update_table(session: Session):
    table_crud.insert_into_table(session, valid_table)

    table_update = Table.Update(id=1, name="2", description="2", source_location="2")
    db_changed_table = table_crud.update_table_on_pk(session, table_update)

    assert db_changed_table.name == "2"
    assert db_changed_table.description == "2"
    assert db_changed_table.source_location == "2"


def test_update_invalid(session: Session):
    table_crud.insert_into_table(session, valid_table)

    with pytest.raises(ValidationError) as exception_info:
        Table.Update(id=1, name=True, description=0.0, source_location={})

    assert len(exception_info.value.errors()) == 3


def test_soft_delete_table(session: Session):
    table_crud.insert_into_table(session, valid_table)
    db_table = table_crud.select_on_pk(session, model_id=1)
    assert db_table.is_active

    table_crud.delete_from_table(session, model_id=1)
    db_deleted_table = table_crud.select_on_pk(session, model_id=1)

    assert not db_deleted_table.is_active


def test_hard_delete_table(session: Session):
    table_crud.insert_into_table(session, valid_table)
    table_crud.delete_from_table(session, model_id=1, hard_delete=True)

    with pytest.raises(ValueError) as exception_info:
        table_crud.select_on_pk(session, 1)

    assert str(exception_info.value) == "404: table with ID: 1 not found."


def test_delete_invalid(session):
    with pytest.raises(ValueError) as exception_info:
        table_crud.delete_from_table(session, 1)

    assert str(exception_info.value) == "404: table with ID: 1 not found."


def test_table_name_source_id_stage_id_unique(session):
    table_crud.insert_into_table(session, valid_table)

    with pytest.raises(IntegrityError) as _:
        table_crud.insert_into_table(session, valid_table)
