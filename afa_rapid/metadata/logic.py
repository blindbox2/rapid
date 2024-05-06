import polars as pl
from ...rapid.rapid_db.models.catalog import (
    Source,
    Stage,
    DataTypeMapping,
    Table,
    Column,
)
from ...rapid.rapid_db.crud.catalog import (
    source_crud,
    stage_crud,
    data_type_mapping_crud,
    table_crud,
    column_crud,
)

from sqlmodel import SQLModel, Session
from typing import List
from ...rapid.rapid_db.crud.base import GenericCrud


def read_metadata_excel(source_location: str, sheet_name: str) -> List[pl.DataFrame]:
    df_excel = pl.read_excel(
        source_location, sheet_name=sheet_name, read_options={"has_header": True}
    )
    return df_excel


def create_objects(df: pl.DataFrame, object_type: SQLModel) -> List[SQLModel]:
    return [object_type.Create(**row) for row in df.rows(named=True)]


def insert_objects(
    session: Session, objects: List[SQLModel], crud_method: GenericCrud
) -> None:
    for object in objects:
        crud_method.insert_into_table(session, object)


def parse_metadata(session: Session):
    metadata_file_path = (
        "/Users/rickdeharder/Code/BDRThermea/platform/test_framework/metadata.xlsx"
    )

    # The order of the arrays below is important, they shoud allign with each other.
    tables_to_parse = ["sources", "stages", "data_type_mappings", "tables", "columns"]
    object_classes = [Source, Stage, DataTypeMapping, Table, Column]
    crud_methods = [
        source_crud,
        stage_crud,
        data_type_mapping_crud,
        table_crud,
        column_crud,
    ]
    data_frames = [
        read_metadata_excel(source_location=metadata_file_path, sheet_name=table)
        for table in tables_to_parse
    ]

    for df, object_class, crud_method in zip(data_frames, object_classes, crud_methods):
        objects = create_objects(df=df, object_type=object_class)
        insert_objects(session, objects=objects, crud_method=crud_method)
