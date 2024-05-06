from fastapi import APIRouter, Depends
import logging
from ...rapid.rapid_db.database import get_session
from ...rapid.rapid_db.models.catalog import Table, Stage
from ...rapid.rapid_db.models.rapid_logging import StageLog
from ...rapid.rapid_db.crud.rapid_logging import stage_log_crud
from ...rapid.rapid_db.crud.catalog import table_crud
from typing import List
from sqlmodel import Session, select
from datetime import datetime
from .logic import ingest_table, enrich_table

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


router = APIRouter(prefix="/orchestration", tags=["orchestration"])


@router.get("/ingest_tables", response_model=List[Table])
def ingest_tables(session: Session = Depends(get_session)):
    db_stage_raw = session.exec(select(Stage).where(Stage.name == "raw")).first()
    tables_to_process = session.exec(
        select(Table).where(Table.stage_id == db_stage_raw.id)
    )

    # Key that indicates the timestamp of ingestion.
    cdc_key = round(datetime.now().timestamp())

    for table in tables_to_process:
        stage_log = StageLog.Open(
            table_id=table.id,
            stage_id=table.stage_id,
            cdc_key=cdc_key,
            run_id="testing",
        )
        db_stage_log = stage_log_crud.open_stage_log(
            session=session, stage_log=stage_log
        )
        db_table = table_crud.select_on_pk(session, model_id=table.id)
        success, number_of_records_processed = ingest_table(session, db_table, cdc_key)
        stage_log = StageLog.Close(
            id=db_stage_log.id,
            success=success,
            number_of_records_processed=number_of_records_processed,
        )
        stage_log_crud.close_stage_log(session=session, stage_log=stage_log)

    return tables_to_process


@router.get("/enrich_tables")
def enrich_tables(cdc_key: int, session: Session = Depends(get_session)):
    db_stage_raw = session.exec(select(Stage).where(Stage.name == "raw")).first()
    db_stage_enriched = session.exec(
        select(Stage).where(Stage.name == "enriched")
    ).first()

    ingestion_to_process: List[StageLog] = session.exec(
        select(StageLog).where(
            StageLog.stage_id == db_stage_raw.id,
            StageLog.number_of_records_processed > 0,
            StageLog.cdc_key == cdc_key,
            StageLog.success,
        )
    ).all()

    for ingestion in ingestion_to_process:
        # check if enriched table exists:
        db_enriched_table = session.exec(
            select(Table).where(
                Table.stage_id == db_stage_enriched.id,
                Table.source_id == ingestion.stage_log_table.source_id,
                Table.name == ingestion.stage_log_table.name,
            )
        ).first()

        if not db_enriched_table:
            db_raw_table = table_crud.select_on_pk(session, ingestion.table_id)
            enriched_table = Table.Create(
                name=db_raw_table.name,
                source_location=f"raw/{db_raw_table.table_source.name}/{db_raw_table.name}",
                stage_id=db_stage_enriched.id,
                source_id=db_raw_table.source_id,
            )
            db_enriched_table = table_crud.insert_into_table(session, enriched_table)

        stage_log = StageLog.Open(
            table_id=db_enriched_table.id,
            stage_id=db_enriched_table.stage_id,
            cdc_key=cdc_key,
            run_id="testing",
        )
        db_stage_log = stage_log_crud.open_stage_log(
            session=session, stage_log=stage_log
        )

        success, number_of_records_processed = enrich_table(db_enriched_table, cdc_key)
        stage_log = StageLog.Close(
            id=db_stage_log.id,
            success=success,
            number_of_records_processed=number_of_records_processed,
        )
        stage_log_crud.close_stage_log(session=session, stage_log=stage_log)
