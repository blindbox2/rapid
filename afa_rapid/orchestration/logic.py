from ...rapid.integrations.excel import ingest_excel
from ...rapid.transformations.excel import enrich_excel
from ...rapid.rapid_db.models.catalog import Table
from typing import Tuple

DATALAKE_ROOT = "/Users/rickdeharder/Code/BDRThermea/platform/test_framework"


def ingest_table(db_table: Table, cdc_key: int) -> Tuple[bool, int]:
    success, number_of_records_ingested = ingest_excel(
        db_table.table_source.name,
        source_location=f"{DATALAKE_ROOT}/data.xlsx",
        table_name=db_table.name,
        cdc_key=cdc_key,
    )
    return success, number_of_records_ingested


def enrich_table(db_table: Table, cdc_key: int) -> Tuple[bool, int]:
    success, number_of_records_ingested = enrich_excel(
        source_location=f"{DATALAKE_ROOT}/{db_table.source_location}",
        cdc_key=cdc_key,
        target_location=f"{DATALAKE_ROOT}/enriched/{db_table.table_source.name}/{db_table.name}",
    )

    return success, number_of_records_ingested
