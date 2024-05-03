from sqlmodel import SQLModel, Field
from datetime import datetime


class RapidTableBase(SQLModel, table=False):
    id: int | None = Field(primary_key=True, default=None)
    datetime_created: datetime | None = Field(default_factory=datetime.now)
    datetime_updated: datetime | None = Field(default_factory=datetime.now)
    is_active: bool = Field(default=True)
