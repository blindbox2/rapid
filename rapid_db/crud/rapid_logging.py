from sqlmodel import Session
from ..models.rapid_logging import StageLog, StageLogMessage
from datetime import datetime


class StageLogCrud:
    def __init__(self):
        self.name = StageLog.__tablename__

    def open_stage_log(
        self, session: Session, stage_log: StageLog.Open
    ) -> StageLog.Return:
        db_stage_log = StageLog.model_validate(stage_log)
        session.add(db_stage_log)
        session.commit()
        session.refresh(db_stage_log)
        return db_stage_log

    def get_stage_log_on_id(
        self, session: Session, stage_log_id: int
    ) -> StageLog.Return:
        db_stage_log = session.get(StageLog, stage_log_id)
        if not db_stage_log:
            raise ValueError(
                f"404: {self.name[:-1]} with ID: {stage_log_id} not found."
            )
        return db_stage_log

    def close_stage_log(
        self, session: Session, stage_log: StageLog.Close
    ) -> StageLog.Return:
        db_stage_log = self.get_stage_log_on_id(session, stage_log.id)
        stage_log_data = stage_log.model_dump(exclude_unset=True)

        # Add the fields to close the log, this is done automatically and not left to the user.
        close_data = {"is_open": False, "datetime_ended": datetime.now()}
        db_stage_log.sqlmodel_update(stage_log_data, update=close_data)

        session.add(db_stage_log)
        session.commit()
        session.refresh(db_stage_log)
        return db_stage_log

    def delete_stage_log(self, session: Session, stage_log_id: int) -> dict:
        db_stage_log = self.get_stage_log_on_id(
            session=session, stage_log_id=stage_log_id
        )

        session.delete(db_stage_log)
        session.commit()

        return {"ok": True}


stage_log_crud = StageLogCrud()


class StageLogMessageCrud:
    def __init__(self):
        self.name = StageLogMessage.__tablename__

    def add_stage_log_message(
        self, session: Session, stage_log_message: StageLogMessage.Add
    ) -> StageLogMessage.Return:
        db_stage_log_message = StageLogMessage.model_validate(stage_log_message)
        session.add(db_stage_log_message)
        session.commit()
        session.refresh(db_stage_log_message)
        return db_stage_log_message

    def get_stage_log_message_on_id(
        self, session: Session, stage_log_message_id: int
    ) -> StageLogMessage.Return:
        db_stage_log_message = session.get(StageLogMessage, stage_log_message_id)
        if not db_stage_log_message:
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

        session.delete(db_stage_log_message)
        session.commit()

        return {"ok": True}


stage_log_message_crud = StageLogMessageCrud()
