from fastapi import APIRouter, Depends, HTTPException
from ...rapid.rapid_db.database import get_session
from ...rapid.rapid_db.crud.rapid_logging import (
    stage_log_crud,
    stage_log_message_crud,
)
from ...rapid.rapid_db.models.rapid_logging import StageLog, StageLogMessage
from sqlmodel import Session
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

router = APIRouter(prefix="/logging", tags=["logging"])


@router.post("/open_stage_log", response_model=StageLog.Return)
def open_stage_log(stage_log: StageLog.Open, session: Session = Depends(get_session)):
    logger.info(
        f"Request received to open a new stage_log for stage_id: {stage_log.stage_id}, and table_id: {stage_log.table_id}"
    )
    try:
        db_stage_log = stage_log_crud.open_stage_log(
            session=session, stage_log=stage_log
        )
        logger.info(f"Succesfully opened a new stage_log with ID: {db_stage_log.id}")
    except Exception as exception:
        logger.error(f"Failed to open a new stage_log with error message: {exception}")
        return HTTPException(
            503,
            detail=f"Failed to open a new stage_log with error message: {exception}",
        )

    return db_stage_log


@router.patch("/close_stage_log", response_model=StageLog.Return)
def close_stage_log(stage_log: StageLog.Close, session=Depends(get_session)):
    logger.info(f"Request received to close stage_log with ID: {stage_log.id}")
    try:
        db_stage_log = stage_log_crud.close_stage_log(
            session=session, stage_log=stage_log
        )
        logger.info(f"Succesfully closed stage_log with ID: {db_stage_log.id}")
    except Exception as exception:
        logger.error(
            f"Failed to close stage_log with ID: {stage_log.id} with error message: {exception}"
        )
        return HTTPException(
            503,
            detail=f"Failed to close stage_log with error message: {exception}",
        )
    return db_stage_log


@router.post("/add_stage_log_message", response_model=StageLogMessage.Return)
def add_stage_log_message(
    stage_log_message: StageLogMessage.Add, session=Depends(get_session)
):
    logging.info(
        f"Adding stage_log_message for stage_log with ID: {stage_log_message.stage_log_id}"
    )
    try:
        db_stage_log_message = stage_log_message_crud.add_stage_log_message(
            session=session, stage_log_message=stage_log_message
        )
        logging.info(
            f"Succesfully added stage_log_message with ID: {db_stage_log_message.id}"
        )
    except Exception as exception:
        logging.error(
            f"Failed to add stage_log_message with error message: {exception}"
        )
        return HTTPException(
            503, f"Failed to add new stage_log_message with error message: {exception}"
        )
    return db_stage_log_message
