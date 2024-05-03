from sqlmodel import Field, Relationship, SQLModel, UniqueConstraint
import sqlalchemy as sa
from typing import List
from .base import RapidTableBase


# Catalog base
class CatalogBase(RapidTableBase, table=False):
    # __table_args__ = [{"schema": "catalog"}]
    pass


class SourceBase(SQLModel, table=False):
    __tablename__ = "sources"
    name: str = Field(max_length=64, sa_type=sa.String(length=64))
    description: str | None = Field(
        max_length=254, sa_type=sa.String(length=254), default=None)
    connection_details: str = Field(sa_type=sa.String(length=254))


class Source(CatalogBase, SourceBase, table=True):
    __table_args__ = (
        UniqueConstraint("name", "connection_details",
                         name="unique_source_name_connection"),
    )

    source_tables: List["Table"] | None = Relationship(
        back_populates="table_source")
    source_data_type_mappings: List["DataTypeMapping"] | None = Relationship(
        back_populates="data_type_mapping_source")

    class Create(SourceBase, table=False):
        pass

    class Return(SourceBase, table=False):
        id: int
        is_active: bool

    class Update(SQLModel, table=False):
        name: str | None = Field(
            max_length=64, sa_type=sa.String(length=64), default=None)
        description: str | None = Field(
            max_length=254, sa_type=sa.String(length=254), default=None)
        connection_details: str | None = Field(
            sa_type=sa.String(length=254), default=None)


class StageBase(SQLModel, table=False):
    __tablename__ = "stages"
    name: str = Field(max_length=64, sa_type=sa.String(length=64), unique=True)
    description: str | None = Field(
        max_length=254, sa_type=sa.String(length=254), default=None)


class Stage(StageBase, CatalogBase, table=True):
    stage_tables: List["Table"] | None = Relationship(
        back_populates="table_stage")

    class Create(StageBase):
        pass

    class Return(StageBase):
        id: int

    class Update(SQLModel, table=False):
        name: str | None = Field(
            max_length=64, sa_type=sa.String(length=64), default=None)
        description: str | None = Field(
            max_length=254, sa_type=sa.String(length=254), default=None)


class TableBase(SQLModel, table=False):
    __tablename__ = "tables"
    name: str = Field(max_length=64, sa_type=sa.String(length=64))
    description: str | None = Field(
        max_length=254, sa_type=sa.String(length=254), default=None)
    source_location: str = Field(max_length=254, sa_type=sa.String(128))


class Table(TableBase, CatalogBase, table=True):
    __table_args__ = (
        UniqueConstraint("name", "source_id",
                         name="unique_table_name_source_id"),
    )
    stage_id: int = Field(foreign_key="stages.id", index=True)
    table_stage: "Stage" = Relationship(back_populates="stage_tables")

    source_id: int = Field(foreign_key="sources.id", index=True)
    table_source: "Source" = Relationship(back_populates="source_tables")

    table_columns: List["Column"] | None = Relationship(
        back_populates="column_table")

    class Create(TableBase, table=False):
        stage_id: int
        source_id: int

    class Return(TableBase, table=False):
        id: int
        is_active: bool

    class Update(SQLModel, table=False):
        name: str | None = Field(
            max_length=64, sa_type=sa.String(length=64), default=None)
        description: str | None = Field(
            max_length=254, sa_type=sa.String(length=254), default=None)
        source_location: str | None = Field(
            sa_type=sa.String(length=128), default=None)


class ColumnBase(SQLModel, table=False):
    __tablename__ = "columns"
    name: str = Field(max_length=64, sa_type=sa.String(length=64))
    description: str | None = Field(
        max_length=254, sa_type=sa.String(length=254), default=None)
    data_type: str = Field(max_length=64, sa_type=sa.String(length=64))
    length: int | None
    nullable: bool | None
    precision: int | None
    scale: int | None
    primary_key: bool = Field(default=False)


class Column(ColumnBase, CatalogBase, table=True):
    __table_args__ = (
        UniqueConstraint("name", "table_id",
                         name="unique_column_name_table_id"),
    )
    table_id: int = Field(foreign_key="tables.id", index=True)
    column_table: "Table" = Relationship(back_populates="table_columns")

    data_type_mapping_id: int = Field(
        foreign_key="data_type_mappings.id", index=True)
    column_data_type_mapping: "DataTypeMapping" = Relationship(
        back_populates="data_type_mapping_columns")

    class Create(ColumnBase, table=False):
        table_id: int
        data_type_mapping_id: int

    class Return(ColumnBase, table=False):
        id: int
        is_active: bool
        table_id: int
        data_type_mapping_id: int

    class Update(SQLModel, table=False):
        name: str | None = Field(
            max_length=64, sa_type=sa.String(length=64), default=None)
        description: str | None = Field(
            max_length=254, sa_type=sa.String(length=254), default=None)
        data_type: str | None = Field(
            max_length=64, sa_type=sa.String(length=64), default=None)
        nullable: bool | None
        length: int | None
        precision: int | None
        scale: int | None
        primary_key: bool | None = Field(default=None)
        data_type_mapping_id: int | None


class DataTypeMappingBase(SQLModel, table=False):
    __tablename__ = "data_type_mappings"
    source_data_type: str = Field(max_length=64, sa_type=sa.String(length=64))
    source_data_format: str | None = Field(
        max_length=128, sa_type=sa.String(length=128), default=None)
    sql_type: str = Field(max_length=64, sa_type=sa.String(length=64))
    parquet_type: str = Field(max_length=64, sa_type=sa.String(length=64))


class DataTypeMapping(DataTypeMappingBase, CatalogBase, table=True):
    __table_args__ = (
        UniqueConstraint("source_data_type", "source_id",
                         name="unique_data_type_mapping_source_data_type_source_id"),
    )

    data_type_mapping_columns: List["Column"] | None = Relationship(
        back_populates="column_data_type_mapping")

    source_id: int = Field(foreign_key="sources.id", index=True)
    data_type_mapping_source: "Source" = Relationship(
        back_populates="source_data_type_mappings")

    class Create(DataTypeMappingBase, table=False):
        source_id: int

    class Return(DataTypeMappingBase, table=False):
        id: int
        source_id: int
        is_active: bool

    class Update(SQLModel, table=False):
        source_data_type: str | None = Field(
            max_length=64, sa_type=sa.String(length=64), default=None)
        source_data_format: str | None = Field(
            max_length=128, sa_type=sa.String(length=128), default=None)
        sql_type: str | None = Field(
            max_length=64, sa_type=sa.String(length=64), default=None)
        parquet_type: str | None = Field(
            max_length=64, sa_type=sa.String(length=64), default=None)
        source_id: int | None
