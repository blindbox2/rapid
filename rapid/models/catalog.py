from sqlmodel import Field, Relationship, SQLModel
import sqlalchemy as sa
from typing import List
from .base import RapidTableBase


# Catalog base
class CatalogBase(RapidTableBase, table=False):
    # __table_args__ = {"schema": "catalog"}
    pass


class SourceBase(SQLModel, table=False):
    __tablename__ = "sources"
    name: str = Field(max_length=64, sa_type=sa.String(length=64))
    description: str | None = Field(max_length=254, sa_type=sa.String(length=254), default=None)
    connection_details: str = Field(sa_type=sa.String(length=254))


class Source(CatalogBase, SourceBase, table=True):
    tables: list["Table"] = Relationship(back_populates="source")

    class Create(SourceBase, table=False):
        pass

    class Return(SourceBase, table=False):
        id: int
        is_active: bool

    class Update(SQLModel, table=False):
        name: str | None = Field(max_length=64, sa_type=sa.String(length=64), default=None)
        description: str | None = Field(max_length=254, sa_type=sa.String(length=254), default=None)
        connection_details: str | None = Field(sa_type=sa.String(length=254), default=None)


class StageBase(SQLModel, table=False):
    __tablename__ = "stages"
    name: str = Field(max_length=64, sa_type=sa.String(length=64))
    description: str | None = Field(max_length=254, sa_type=sa.String(length=254), default=None)


class Stage(StageBase, CatalogBase, table=True):
    tables: List["Table"] = Relationship(back_populates="stage")

    class Create(StageBase):
        pass

    class Return(StageBase):
        id: int

    class Update(SQLModel, table=False):
        name: str | None = Field(max_length=64, sa_type=sa.String(length=64), default=None)
        description: str | None = Field(max_length=254, sa_type=sa.String(length=254), default=None)


class TableBase(SQLModel, table=False):
    __tablename__ = "tables"
    name: str = Field(max_length=64, sa_type=sa.String(length=64))
    description: str | None = Field(max_length=254, sa_type=sa.String(length=254), default=None)
    source_location: str = Field(max_length=254, sa_type=sa.String(128))


class Table(TableBase, CatalogBase, table=True):
    stage_id: int | None = Field(default=None, foreign_key="stages.id")
    stage: "Stage" = Relationship(back_populates="tables")

    source_id: int | None = Field(default=None, foreign_key="sources.id")
    source: "Source" = Relationship(back_populates="tables")

    columns: List["Column"] = Relationship(back_populates="table")

    class Create(TableBase, table=False):
        stage_id: int
        source_id: int

    class Return(TableBase, table=False):
        id: int
        is_active: bool

    class Update(SQLModel, table=False):
        name: str | None = Field(max_length=64, sa_type=sa.String(length=64), default=None)
        description: str | None = Field(max_length=254, sa_type=sa.String(length=254), default=None)
        source_location: str | None = Field(sa_type=sa.String(length=128), default=None)


class ColumnBase(SQLModel, table=False):
    __tablename__ = "columns"
    name: str = Field(max_length=64, sa_type=sa.String(length=64))
    description: str | None = Field(max_length=254, sa_type=sa.String(length=254), default=None)
    data_type: str = Field(max_length=64, sa_type=sa.String(length=64))
    nullable: bool | None
    precision: int | None
    scale: int | None
    primary_key: bool = Field(default=False)


class Column(ColumnBase, CatalogBase, table=True):
    table_id: int | None = Field(default=None, foreign_key="tables.id")
    table: "Table" = Relationship(back_populates="columns")

    data_type_id: int | None = Field(default=None, foreign_key="data_type_mappings.id")
    data_type_mapping: "DataTypeMapping" = Relationship(back_populates="column_type_mapping")

    class Create(ColumnBase, table=False):
        pass

    class Return(ColumnBase, table=False):
        id: int
        is_active: bool

    class Update(SQLModel, table=False):
        name: str | None = Field(max_length=64, sa_type=sa.String(length=64), default=None)
        description: str | None = Field(max_length=254, sa_type=sa.String(length=254), default=None)
        data_type: str | None = Field(max_length=64, sa_type=sa.String(length=64), default=None)
        nullable: bool | None
        precision: int | None
        scale: int | None
        primary_key: bool | None = Field(default=None)


class DataTypeMappingBase(SQLModel, table=False):
    __tablename__ = "data_type_mappings"
    source_data_type: str = Field(max_length=64, sa_type=sa.String(length=64))
    source_data_format: str = Field(max_length=128, sa_type=sa.String(length=128))
    sql_type: str = Field(max_length=64, sa_type=sa.String(length=64))
    parquet_type: str = Field(max_length=64, sa_type=sa.String(length=64))


class DataTypeMapping(DataTypeMappingBase, CatalogBase, table=True):
    column_type_mapping: "Column" = Relationship(back_populates="data_type_mapping")

    class Create(DataTypeMappingBase, table=False):
        pass

    class Return(DataTypeMappingBase, table=False):
        id: int
        is_active: bool

    class Update(SQLModel, table=False):
        source_data_type: str | None = Field(max_length=64, sa_type=sa.String(length=64), default=None)
        source_data_format: str | None = Field(max_length=128, sa_type=sa.String(length=128), default=None)
        sql_type: str | None = Field(max_length=64, sa_type=sa.String(length=64), default=None)
        parquet_type: str | None = Field(max_length=64, sa_type=sa.String(length=64), default=None)
