import pytest
from sqlmodel import Session, SQLModel, create_engine
from ...crud.catalog import table_crud
from ...models.catalog import Table
from pydantic import ValidationError


@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine(
        "sqlite://"
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


valid_table = Table.Create(name="1", description="1", source_location="1", stage_id=1, source_id=1)


def test_create_table(session: Session):
    created_table = table_crud.create_model(session, valid_table)

    assert created_table.id == 1
    assert created_table.name == "1"
    assert created_table.description == "1"
    assert created_table.source_location == "1"
    assert created_table.stage_id == 1
    assert created_table.source_id == 1


def test_create_invalid_table():
    with pytest.raises(ValidationError) as exception_info:
        _ = Table.Create(name=1, description=1, source_location=1, stage_id=[], source_id={})

    assert len(exception_info.value.errors()) == 5


def test_get_table(session: Session):
    _ = table_crud.create_model(session, valid_table)

    db_table = table_crud.get_model_on_id(session, 1)

    assert db_table.id == 1
    assert db_table.name == "1"
    assert db_table.description == "1"
    assert db_table.source_location == "1"


def test_get_table_invalid(session: Session):
    with pytest.raises(ValueError) as exception_info:
        table_crud.get_model_on_id(session, model_id=1)

    assert str(exception_info.value) == f"404: table with ID: 1 not found."


def test_get_tables(session: Session):
    table1 = Table.Create(
        name="2", description="2", source_location="2", source_id=2, stage_id=2)

    _ = table_crud.create_model(session, valid_table)
    _ = table_crud.create_model(session, table1)

    db_tables = table_crud.get_all_models(session)
    assert len(db_tables) == 2
    assert db_tables[0].id == 1
    assert db_tables[1].id == 2


def test_get_tables_invalid(session: Session):
    with pytest.raises(ValueError) as exception_info:
        table_crud.get_all_models(session)

    assert str(exception_info.value) == f"404: no tables found."


def test_update_table(session: Session):
    db_table = table_crud.create_model(session, valid_table)

    table_update = Table.Update(name="2", description="2", source_location="2")
    db_changed_table = table_crud.update_model(session, db_table.id, table_update)

    assert db_changed_table.name == "2"
    assert db_changed_table.description == "2"
    assert db_changed_table.source_location == "2"

    db_changed_table_from_db = table_crud.get_model_on_id(session, model_id=db_table.id)

    assert db_changed_table_from_db.name == "2"
    assert db_changed_table_from_db.description == "2"
    assert db_changed_table_from_db.source_location == "2"


def test_update_invalid(session: Session):
    _ = table_crud.create_model(session, valid_table)

    with pytest.raises(ValidationError) as exception_info:
        _ = Table.Update(name=True, description=0.0, source_location={})

    assert len(exception_info.value.errors()) == 3


def test_soft_delete_table(session: Session):
    table_crud.create_model(session, valid_table)

    db_table = table_crud.get_model_on_id(session, model_id=1)
    assert db_table.is_active

    table_crud.delete_model(session, model_id=1)
    db_deleted_table = table_crud.get_model_on_id(session, model_id=1)

    assert not db_deleted_table.is_active


def test_hard_delete_table(session: Session):
    _ = table_crud.create_model(session, valid_table)

    db_table = table_crud.get_model_on_id(session, model_id=1)
    assert db_table.is_active

    table_crud.delete_model(session, model_id=1, hard_delete=True)

    with pytest.raises(ValueError) as exception_info:
        table_crud.get_model_on_id(session, 1)

    assert str(exception_info.value) == f"404: table with ID: 1 not found."


def test_delete_invalid(session):
    with pytest.raises(ValueError) as exception_info:
        table_crud.delete_model(session, 1)

    assert str(exception_info.value) == f"404: table with ID: 1 not found."
