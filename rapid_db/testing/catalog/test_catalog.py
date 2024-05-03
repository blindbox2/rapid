import pytest
from sqlmodel import Session, SQLModel, create_engine
from ...models.catalog import Source, Table, Stage, Column, DataTypeMapping
from ...crud.catalog import source_crud, table_crud, stage_crud, column_crud, data_type_mapping_crud


@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine(
        "sqlite://"
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


def test_source_table_relation(session: Session):
    source = Source.Create(name="test source", description="test source description",
                           connection_details="test connection details")
    db_source = source_crud.create_model(session, source)

    table = Table.Create(
        name="test table",
        description="test table description",
        source_location="test source location",
        stage_id=9,
        source_id=db_source.id,
    )

    table1 = Table.Create(
        name="test table1",
        description="test table description1",
        source_location="test source location1",
        stage_id=9,
        source_id=db_source.id,
    )

    db_table = table_crud.create_model(session, table)
    _ = table_crud.create_model(session, table1)

    # Check that the source has related tables (instead of "Check that the stage has a related table")
    source = source_crud.get_model_on_id(session, model_id=db_source.id)
    assert len(source.tables) == 2
    assert source.tables[0].id == db_table.id

    # Check that the table has a related stage
    table = table_crud.get_model_on_id(session, model_id=db_table.id)
    assert table.source_id == source.id
    assert table.source.id == source.id

    # Check that the relation is correctly set up in both directions
    assert table in source.tables
    assert source is table.source


def test_stage_table_relation(session: Session):
    stage = Stage.Create(name="test stage", description="test stage description")
    db_stage = stage_crud.create_model(session, stage)

    table = Table.Create(
        name="test table",
        description="test table description",
        source_location="test source location",
        stage_id=db_stage.id,
        source_id=9
    )

    table1 = Table.Create(
        name="test table1",
        description="test table description1",
        source_location="test source location1",
        stage_id=db_stage.id,
        source_id=9,
    )

    db_table = table_crud.create_model(session, table)
    _ = table_crud.create_model(session, table1)

    # Check that the stage has related tables
    stage = stage_crud.get_model_on_id(session, model_id=db_stage.id)
    assert len(stage.tables) == 2
    assert stage.tables[0].id == db_table.id

    # Check that the table has a related stage
    table = table_crud.get_model_on_id(session, model_id=db_table.id)
    assert table.stage_id == stage.id
    assert table.stage.id == stage.id

    # Check that the relation is correctly set up in both directions
    assert table in stage.tables
    assert stage is table.stage


def test_table_column_relation(session: Session):
    table = Table.Create(
        name="test table",
        description="test table description",
        source_location="test source location",
        stage_id=9,
        source_id=9
    )
    db_table = table_crud.create_model(session, table)

    column = Column.Create(
        name="test column",
        description="test column description",
        data_type="test datatype",
        nullable=True,
        precision=1,
        length=1,
        scale=1,
        primary_key=True,
        table_id=db_table.id,
        data_type_mapping_id=9
    )

    column1 = Column.Create(
        name="test column1",
        description="test column1 description",
        data_type="test datatype",
        nullable=True,
        precision=1,
        length=1,
        scale=1,
        primary_key=True,
        table_id=db_table.id,
        data_type_mapping_id=9
    )

    _ = column_crud.create_model(session, column)
    _ = column_crud.create_model(session, column1)

    # Check that the table has related columns
    table = table_crud.get_model_on_id(session, model_id=db_table.id)
    assert len(table.columns) == 2
    assert table.columns[0].id == db_table.id

    # Check that the column has a related table
    column = column_crud.get_model_on_id(session, model_id=db_table.id)
    assert column.table_id == table.id
    assert column.table.id == table.id

    # Check that the relation is correctly set up in both directions
    assert column in table.columns
    assert table is column.table


def test_data_type_mapping_column_relation(session: Session):
    data_type_mapping = DataTypeMapping.Create(
        source_data_type="test source data type",
        source_data_format="test source data format",
        sql_type="test sql type",
        parquet_type="test parquet type"
    )
    db_data_type_mapping = data_type_mapping_crud.create_model(session, data_type_mapping)

    column = Column.Create(
        name="test column",
        description="test column description",
        data_type="test datatype",
        nullable=True,
        precision=1,
        length=1,
        scale=1,
        primary_key=True,
        table_id=9,
        data_type_mapping_id=db_data_type_mapping.id
    )

    _ = column_crud.create_model(session, column)

    # Check that the data_type_mapping has related columns
    data_type_mapping = data_type_mapping_crud.get_model_on_id(session, model_id=db_data_type_mapping.id)
    assert len(data_type_mapping.column_type_mappings) == 1
    assert data_type_mapping.column_type_mappings[0].id == db_data_type_mapping.id

    # Check that the column has a related data_type_mapping
    column = column_crud.get_model_on_id(session, model_id=db_data_type_mapping.id)
    assert column.data_type_mapping_id == data_type_mapping.id
    assert column.data_type_mapping.id == data_type_mapping.id

    # Check that the relation is correctly set up in both directions
    assert column in data_type_mapping.column_type_mappings
    assert data_type_mapping is column.data_type_mapping
