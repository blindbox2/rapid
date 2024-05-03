from .base import GenericCrud
from ..models.catalog import Source, Stage, Table, Column, DataTypeMapping


class SourceCrud(GenericCrud[Source, Source.Create, Source.Update, Source.Return]):
    def __init__(self):
        super().__init__(model=Source)


source_crud = SourceCrud()


class StageCrud(GenericCrud[Stage, Stage.Create, Stage.Update, Stage.Return]):
    def __init__(self):
        super().__init__(model=Stage)


stage_crud = StageCrud()


class TableCrud(GenericCrud[Table, Table.Create, Table.Update, Table.Return]):
    def __init__(self):
        super().__init__(model=Table)


table_crud = TableCrud()


class ColumnCrud(GenericCrud[Column, Column.Create, Column.Update, Column.Return]):
    def __init__(self):
        super().__init__(model=Column)


column_crud = ColumnCrud()


class DataTypeMappingCrud(
    GenericCrud[
        DataTypeMapping,
        DataTypeMapping.Create,
        DataTypeMapping.Update,
        DataTypeMapping.Return]
):
    def __init__(self):
        super().__init__(model=DataTypeMapping)


data_type_mapping_crud = DataTypeMappingCrud()
