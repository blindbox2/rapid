import pytest
from sqlmodel import Session, SQLModel, create_engine
from ...crud.catalog import column_crud
from ...models.catalog import Column
from pydantic import ValidationError


@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine(
        "sqlite://"
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


valid_column = Column.Create(name="1", data_type="1", nullable=True, length=1, precision=1, scale=1, primary_key=True, table_id=1,
                             data_type_mapping_id=1)


def test_create_column(session: Session):
    created_column = column_crud.create_model(session, valid_column)

    assert created_column.id == 1
    assert created_column.name == "1"
    assert created_column.data_type == "1"
    assert created_column.nullable
    assert created_column.length == 1
    assert created_column.precision == 1
    assert created_column.scale == 1
    assert created_column.primary_key


def test_create_invalid_column():
    with pytest.raises(ValidationError) as exception_info:
        _ = Column.Create(name=1, data_type=2, nullable=3, length=[], precision={}, scale=[], primary_key='', table_id=[],
                          data_type_mapping_id={})

    assert len(exception_info.value.errors()) == 9


def test_get_column(session: Session):
    _ = column_crud.create_model(session, valid_column)

    db_column = column_crud.get_model_on_id(session, 1)

    assert db_column.id == 1
    assert db_column.name == "1"
    assert db_column.data_type == "1"
    assert db_column.nullable
    assert db_column.length == 1
    assert db_column.precision == 1
    assert db_column.scale == 1
    assert db_column.primary_key
    assert db_column.table_id == 1
    assert db_column.data_type_mapping_id == 1


def test_get_column_invalid(session: Session):
    with pytest.raises(ValueError) as exception_info:
        column_crud.get_model_on_id(session, model_id=1)

    assert str(exception_info.value) == f"404: column with ID: 1 not found."


def test_get_columns(session: Session):
    column1 = Column.Create(name="3", data_type="3", nullable=True, length=3, precision=3, scale=3, primary_key=True, table_id=3,
                            data_type_mapping_id=3)

    _ = column_crud.create_model(session, valid_column)
    _ = column_crud.create_model(session, column1)

    db_columns = column_crud.get_all_models(session)
    assert len(db_columns) == 2
    assert db_columns[0].id == 1
    assert db_columns[1].id == 2


def test_get_columns_invalid(session: Session):
    with pytest.raises(ValueError) as exception_info:
        column_crud.get_all_models(session)

    assert str(exception_info.value) == f"404: no columns found."


def test_update_column(session: Session):
    db_column = column_crud.create_model(session, valid_column)

    column_update = Column.Update(name="2", data_type="2", nullable=False, length=2, precision=2, scale=2, primary_key=False,
                                  data_type_mapping_id=2)
    db_changed_column = column_crud.update_model(session, db_column.id, column_update)

    assert db_changed_column.name == "2"
    assert db_changed_column.data_type == "2"
    assert not db_changed_column.nullable
    assert db_changed_column.length == 2
    assert db_changed_column.precision == 2
    assert db_changed_column.scale == 2
    assert db_changed_column.data_type_mapping_id == 2
    assert not db_changed_column.primary_key

    db_changed_column_from_db = column_crud.get_model_on_id(session, model_id=db_column.id)

    assert db_changed_column_from_db.name == "2"
    assert db_changed_column_from_db.data_type == "2"
    assert not db_changed_column_from_db.nullable
    assert db_changed_column_from_db.length == 2
    assert db_changed_column_from_db.precision == 2
    assert db_changed_column_from_db.scale == 2
    assert db_changed_column_from_db.data_type_mapping_id == 2
    assert not db_changed_column_from_db.primary_key


def test_update_invalid(session: Session):
    _ = column_crud.create_model(session, valid_column)

    with pytest.raises(ValidationError) as exception_info:
        _ = Column.Update(name=True, data_type=0.0, length={}, nullable=4, precision={}, scale='', primary_key=3,
                          data_type_mapping_id='9')

    assert len(exception_info.value.errors()) == 7


def test_soft_delete_column(session: Session):
    column_crud.create_model(session, valid_column)

    db_column = column_crud.get_model_on_id(session, model_id=1)
    assert db_column.is_active

    column_crud.delete_model(session, model_id=1)
    db_deleted_column = column_crud.get_model_on_id(session, model_id=1)

    assert not db_deleted_column.is_active


def test_hard_delete_column(session: Session):
    _ = column_crud.create_model(session, valid_column)

    db_column = column_crud.get_model_on_id(session, model_id=1)
    assert db_column.is_active

    column_crud.delete_model(session, model_id=1, hard_delete=True)

    with pytest.raises(ValueError) as exception_info:
        column_crud.get_model_on_id(session, 1)

    assert str(exception_info.value) == f"404: column with ID: 1 not found."


def test_delete_invalid(session):
    with pytest.raises(ValueError) as exception_info:
        column_crud.delete_model(session, 1)

    assert str(exception_info.value) == f"404: column with ID: 1 not found."
