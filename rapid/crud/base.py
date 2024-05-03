from sqlmodel import Session, select
from typing import Generic, TypeVar, List

ModelType = TypeVar("ModelType", bound="SQLModel")
CreateSchemaType = TypeVar("CreateSchemaType", bound="SQLModel")
UpdateSchemaType = TypeVar("UpdateSchemaType", bound="SQLModel")
ReturnSchemaType = TypeVar("ReturnSchemaType", bound="SQLModel")


class GenericCrud(Generic[ModelType, CreateSchemaType, UpdateSchemaType, ReturnSchemaType]):
    def __init__(self, model: ModelType):
        self.model = model
        self.name = model.__tablename__

    def create_model(self, session: Session, model: CreateSchemaType) -> ReturnSchemaType:
        db_model = self.model.model_validate(model)
        session.add(db_model)
        session.commit()
        session.refresh(db_model)
        return db_model

    def get_all_models(self, session: Session, offset: int = 0, limit: int = 100) -> List[ReturnSchemaType]:
        models = session.exec(
            select(self.model).offset(offset).limit(limit)).all()
        if len(models) == 0:
            raise ValueError(f"404: no {self.name} found.")
        return models

    def get_model_on_id(self, session: Session, model_id: int) -> ReturnSchemaType:
        db_model = session.get(self.model, model_id)
        if not db_model:
            raise ValueError(
                f"404: {self.name[:-1]} with ID: {model_id} not found.")
        return db_model

    def update_model(self, session: Session, model_id: int, model: UpdateSchemaType) -> ReturnSchemaType:
        db_model = self.get_model_on_id(session, model_id)
        model_data = model.model_dump(exclude_unset=True)
        db_model.sqlmodel_update(model_data)
        session.add(db_model)
        session.commit()
        session.refresh(db_model)
        return db_model

    def delete_model(self, session: Session, model_id: int, hard_delete: bool = False) -> dict:
        db_model = self.get_model_on_id(session, model_id)

        if hard_delete:
            session.delete(db_model)
        else:
            db_model.is_active = False
            session.add(db_model)

        session.commit()
        return {"ok": True}
