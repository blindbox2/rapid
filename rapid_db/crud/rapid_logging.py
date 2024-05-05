import logging
from sqlmodel import Session
from ..models.rapid_logging import StageLog, StageLogMessage
from datetime import datetime

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


class StageLogCrud:
    def __init__(self):
        self.name = StageLog.__tablename__

    def open_stage_log(
        self, session: Session, stage_log: StageLog.Open
    ) -> StageLog.Return:
        logger.info("Opening a new stage_log")
        db_stage_log = StageLog.model_validate(stage_log)
        session.add(db_stage_log)
        session.commit()
        session.refresh(db_stage_log)
        logger.info(f"Opened a new stage_log with ID: {db_stage_log.id}")
        return db_stage_log

    def get_stage_log_on_id(
        self, session: Session, stage_log_id: int
    ) -> StageLog.Return:
        db_stage_log = session.get(StageLog, stage_log_id)
        if not db_stage_log:
            logger.warning(f"No {self.name[:-1]} found with ID: {stage_log_id}")
            raise ValueError(
                f"404: {self.name[:-1]} with ID: {stage_log_id} not found."
            )
        return db_stage_log

    def close_stage_log(
        self, session: Session, stage_log: StageLog.Close
    ) -> StageLog.Return:
        logger.info(f"Closing stage_log with ID: {stage_log.id}")
        db_stage_log = self.get_stage_log_on_id(session, stage_log.id)
        stage_log_data = stage_log.model_dump(exclude_unset=True)

        # Add the fields to close the log, this is done automatically and not left to the user.
        close_data = {"is_open": False, "datetime_ended": datetime.now()}
        db_stage_log.sqlmodel_update(stage_log_data, update=close_data)

        session.add(db_stage_log)
        session.commit()
        session.refresh(db_stage_log)
        logger.info(f"Closed stage_log with ID: {stage_log.id}")
        return db_stage_log

    def delete_stage_log(self, session: Session, stage_log_id: int) -> dict:
        db_stage_log = self.get_stage_log_on_id(
            session=session, stage_log_id=stage_log_id
        )
        logger.info(f"Deleting stage_log with ID: {stage_log_id}")

        session.delete(db_stage_log)
        session.commit()
        logger.info(f"Deleted stage_log with ID: {stage_log_id}")

        return {"ok": True}


stage_log_crud = StageLogCrud()


class StageLogMessageCrud:
    def __init__(self):
        self.name = StageLogMessage.__tablename__

    def add_stage_log_message(
        self, session: Session, stage_log_message: StageLogMessage.Add
    ) -> StageLogMessage.Return:
        logger.info(
            f"Adding a new stage_log_message for stage_log with ID: {stage_log_message.stage_log_id}"
        )
        db_stage_log_message = StageLogMessage.model_validate(stage_log_message)
        session.add(db_stage_log_message)
        session.commit()
        session.refresh(db_stage_log_message)
        logger.info(
            f"Added a new stage_log_message for stage_log with ID: {stage_log_message.stage_log_id}"
        )
        return db_stage_log_message

    def get_stage_log_message_on_id(
        self, session: Session, stage_log_message_id: int
    ) -> StageLogMessage.Return:
        db_stage_log_message = session.get(StageLogMessage, stage_log_message_id)
        if not db_stage_log_message:
            logger.warning("No {self.name[:-1]} with ID: {stage_log_message_id} found.")
            raise ValueError(
                f"404: {self.name[:-1]} with ID: {stage_log_message_id} not found."
            )
        return db_stage_log_message

    def delete_stage_log_message(
        self, session: Session, stage_log_message_id: int
    ) -> dict:
        db_stage_log_message = self.get_stage_log_message_on_id(
            session=session, stage_log_message_id=stage_log_message_id
        )
        logger.info(f"Deleting {self.name[:-1]} with ID: {stage_log_message_id}")

        session.delete(db_stage_log_message)
        session.commit()
        logger.info(f"Deleted {self.name[:-1]} with ID: {stage_log_message_id}")

        return {"ok": True}


stage_log_message_crud = StageLogMessageCrud()
