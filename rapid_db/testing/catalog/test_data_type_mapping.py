import pytest
from sqlmodel import Session
from ...crud.catalog import data_type_mapping_crud
from ...models.catalog import DataTypeMapping
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError


valid_data_type_mapping = DataTypeMapping.Create(
    source_data_type="1",
    source_data_format="1",
    sql_type="1",
    parquet_type="1",
    source_id=1,
)


def test_create_data_type_mapping(session: Session):
    created_data_type_mapping = data_type_mapping_crud.insert_into_table(
        session, valid_data_type_mapping
    )

    assert created_data_type_mapping.id == 1
    assert created_data_type_mapping.source_id == 1
    assert created_data_type_mapping.source_data_type == "1"
    assert created_data_type_mapping.source_data_format == "1"
    assert created_data_type_mapping.sql_type == "1"
    assert created_data_type_mapping.parquet_type == "1"


def test_create_invalid_data_type_mapping():
    with pytest.raises(ValidationError) as exception_info:
        DataTypeMapping.Create(
            source_data_type=False,
            source_data_format=1,
            sql_type=4,
            parquet_type=3,
            source_id="a",
        )

    assert len(exception_info.value.errors()) == 5


def test_get_data_type_mapping(session: Session):
    data_type_mapping_crud.insert_into_table(session, valid_data_type_mapping)
    db_data_type_mapping = data_type_mapping_crud.select_on_pk(session, 1)

    assert db_data_type_mapping.id == 1
    assert db_data_type_mapping.source_id == 1
    assert db_data_type_mapping.source_data_type == "1"
    assert db_data_type_mapping.source_data_format == "1"
    assert db_data_type_mapping.sql_type == "1"
    assert db_data_type_mapping.parquet_type == "1"


def test_get_data_type_mapping_invalid(session: Session):
    with pytest.raises(ValueError) as exception_info:
        data_type_mapping_crud.select_on_pk(session, model_id=1)

    assert str(exception_info.value) == "404: data_type_mapping with ID: 1 not found."


def test_get_data_type_mappings(session: Session):
    data_type_mapping1 = DataTypeMapping.Create(
        source_data_type="2",
        source_data_format="2",
        sql_type="2",
        parquet_type="2",
        source_id=2,
    )

    data_type_mapping_crud.insert_into_table(session, valid_data_type_mapping)
    data_type_mapping_crud.insert_into_table(session, data_type_mapping1)

    db_data_type_mappings = data_type_mapping_crud.select_all(session)
    assert len(db_data_type_mappings) == 2
    assert db_data_type_mappings[0].id == 1
    assert db_data_type_mappings[1].id == 2


def test_get_data_type_mappings_invalid(session: Session):
    with pytest.raises(ValueError) as exception_info:
        data_type_mapping_crud.select_all(session)

    assert str(exception_info.value) == "404: no data_type_mappings found."


def test_update_data_type_mapping(session: Session):
    data_type_mapping_crud.insert_into_table(session, valid_data_type_mapping)

    data_type_mapping_update = DataTypeMapping.Update(
        id=1,
        source_data_type="2",
        source_data_format="2",
        sql_type="2",
        parquet_type="2",
        source_id=2,
    )
    db_changed_data_type_mapping = data_type_mapping_crud.update_table_on_pk(
        session, data_type_mapping_update
    )

    assert db_changed_data_type_mapping.source_data_type == "2"
    assert db_changed_data_type_mapping.source_id == 2
    assert db_changed_data_type_mapping.source_data_format == "2"
    assert db_changed_data_type_mapping.sql_type == "2"
    assert db_changed_data_type_mapping.parquet_type == "2"


def test_update_invalid(session: Session):
    data_type_mapping_crud.insert_into_table(session, valid_data_type_mapping)

    with pytest.raises(ValidationError) as exception_info:
        DataTypeMapping.Update(
            id="a",
            source_data_type=False,
            source_data_format=1,
            sql_type=4,
            parquet_type=3,
            source_id="a",
        )

    assert len(exception_info.value.errors()) == 6


def test_soft_delete_data_type_mapping(session: Session):
    data_type_mapping_crud.insert_into_table(session, valid_data_type_mapping)

    db_data_type_mapping = data_type_mapping_crud.select_on_pk(session, model_id=1)
    assert db_data_type_mapping.is_active

    data_type_mapping_crud.delete_from_table(session, model_id=1)
    db_deleted_data_type_mapping = data_type_mapping_crud.select_on_pk(
        session, model_id=1
    )

    assert not db_deleted_data_type_mapping.is_active


def test_hard_delete_data_type_mapping(session: Session):
    data_type_mapping_crud.insert_into_table(session, valid_data_type_mapping)
    data_type_mapping_crud.delete_from_table(session, model_id=1, hard_delete=True)

    with pytest.raises(ValueError) as exception_info:
        data_type_mapping_crud.select_on_pk(session, 1)

    assert str(exception_info.value) == "404: data_type_mapping with ID: 1 not found."


def test_delete_invalid(session):
    with pytest.raises(ValueError) as exception_info:
        data_type_mapping_crud.delete_from_table(session, 1)

    assert str(exception_info.value) == "404: data_type_mapping with ID: 1 not found."


def test_name_source_id_unique(session):
    data_type_mapping_crud.insert_into_table(session, valid_data_type_mapping)

    with pytest.raises(IntegrityError) as _:
        data_type_mapping_crud.insert_into_table(session, valid_data_type_mapping)
