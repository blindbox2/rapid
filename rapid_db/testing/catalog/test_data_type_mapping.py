import pytest
from sqlmodel import Session, SQLModel, create_engine
from ...crud.catalog import data_type_mapping_crud
from ...models.catalog import DataTypeMapping
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError


@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine(
        "sqlite://"
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


valid_data_type_mapping = DataTypeMapping.Create(source_data_type='1', source_data_format='1', sql_type='1',
                                                 parquet_type='1', source_id=1)


def test_create_data_type_mapping(session: Session):
    created_data_type_mapping = data_type_mapping_crud.create_model(session, valid_data_type_mapping)

    assert created_data_type_mapping.id == 1
    assert created_data_type_mapping.source_id == 1
    assert created_data_type_mapping.source_data_type == "1"
    assert created_data_type_mapping.source_data_format == "1"
    assert created_data_type_mapping.sql_type == "1"
    assert created_data_type_mapping.parquet_type == "1"


def test_create_invalid_data_type_mapping():
    with pytest.raises(ValidationError) as exception_info:
        _ = DataTypeMapping.Create(source_data_type=False, source_data_format=1, sql_type=4, parquet_type=3,
                                   source_id="a")

    assert len(exception_info.value.errors()) == 5


def test_get_data_type_mapping(session: Session):
    _ = data_type_mapping_crud.create_model(session, valid_data_type_mapping)

    db_data_type_mapping = data_type_mapping_crud.get_model_on_id(session, 1)

    assert db_data_type_mapping.id == 1
    assert db_data_type_mapping.source_id == 1
    assert db_data_type_mapping.source_data_type == "1"
    assert db_data_type_mapping.source_data_format == "1"
    assert db_data_type_mapping.sql_type == "1"
    assert db_data_type_mapping.parquet_type == "1"


def test_get_data_type_mapping_invalid(session: Session):
    with pytest.raises(ValueError) as exception_info:
        data_type_mapping_crud.get_model_on_id(session, model_id=1)

    assert str(exception_info.value) == f"404: data_type_mapping with ID: 1 not found."


def test_get_data_type_mappings(session: Session):
    data_type_mapping1 = DataTypeMapping.Create(source_data_type='2', source_data_format='2', sql_type="2",
                                                parquet_type="2", source_id=2)

    _ = data_type_mapping_crud.create_model(session, valid_data_type_mapping)
    _ = data_type_mapping_crud.create_model(session, data_type_mapping1)

    db_data_type_mappings = data_type_mapping_crud.get_all_models(session)
    assert len(db_data_type_mappings) == 2
    assert db_data_type_mappings[0].id == 1
    assert db_data_type_mappings[1].id == 2


def test_get_data_type_mappings_invalid(session: Session):
    with pytest.raises(ValueError) as exception_info:
        data_type_mapping_crud.get_all_models(session)

    assert str(exception_info.value) == f"404: no data_type_mappings found."


def test_update_data_type_mapping(session: Session):
    db_data_type_mapping = data_type_mapping_crud.create_model(session, valid_data_type_mapping)

    data_type_mapping_update = DataTypeMapping.Update(source_data_type="2", source_data_format="2", sql_type="2",
                                                      parquet_type="2", source_id=2)
    db_changed_data_type_mapping = data_type_mapping_crud.update_model(session, db_data_type_mapping.id,
                                                                       data_type_mapping_update)

    assert db_changed_data_type_mapping.source_data_type == "2"
    assert db_changed_data_type_mapping.source_id == 2
    assert db_changed_data_type_mapping.source_data_format == "2"
    assert db_changed_data_type_mapping.sql_type == "2"
    assert db_changed_data_type_mapping.parquet_type == "2"

    _ = data_type_mapping_crud.get_model_on_id(session, model_id=db_data_type_mapping.id)

    assert db_changed_data_type_mapping.source_data_type == "2"
    assert db_changed_data_type_mapping.source_id == 2
    assert db_changed_data_type_mapping.source_data_format == "2"
    assert db_changed_data_type_mapping.sql_type == "2"
    assert db_changed_data_type_mapping.parquet_type == "2"


def test_update_invalid(session: Session):
    _ = data_type_mapping_crud.create_model(session, valid_data_type_mapping)

    with pytest.raises(ValidationError) as exception_info:
        _ = DataTypeMapping.Update(source_data_type=False, source_data_format=1, sql_type=4, parquet_type=3,
                                   source_id="a")

    assert len(exception_info.value.errors()) == 5


def test_soft_delete_data_type_mapping(session: Session):
    data_type_mapping_crud.create_model(session, valid_data_type_mapping)

    db_data_type_mapping = data_type_mapping_crud.get_model_on_id(session, model_id=1)
    assert db_data_type_mapping.is_active

    data_type_mapping_crud.delete_model(session, model_id=1)
    db_deleted_data_type_mapping = data_type_mapping_crud.get_model_on_id(session, model_id=1)

    assert not db_deleted_data_type_mapping.is_active


def test_hard_delete_data_type_mapping(session: Session):
    _ = data_type_mapping_crud.create_model(session, valid_data_type_mapping)

    db_data_type_mapping = data_type_mapping_crud.get_model_on_id(session, model_id=1)
    assert db_data_type_mapping.is_active

    data_type_mapping_crud.delete_model(session, model_id=1, hard_delete=True)

    with pytest.raises(ValueError) as exception_info:
        data_type_mapping_crud.get_model_on_id(session, 1)

    assert str(exception_info.value) == f"404: data_type_mapping with ID: 1 not found."


def test_delete_invalid(session):
    with pytest.raises(ValueError) as exception_info:
        data_type_mapping_crud.delete_model(session, 1)

    assert str(exception_info.value) == f"404: data_type_mapping with ID: 1 not found."


def test_name_source_id_unique(session):
    created_table = data_type_mapping_crud.create_model(session, valid_data_type_mapping)
    
    with pytest.raises(IntegrityError) as _:        
        created_table = data_type_mapping_crud.create_model(session, valid_data_type_mapping)
