from sqlmodel import create_engine, Session, SQLModel
from sqlalchemy import event

sqlite_file_name = "rapid_db.db"

sqlite_url = f"sqlite:///{sqlite_file_name}"
engine = create_engine(sqlite_url, echo=True)


def _fk_pragma_on_connect(dbapi_con, con_record):
    dbapi_con.execute("pragma foreign_keys=ON")


event.listen(engine, "connect", _fk_pragma_on_connect)


def get_session():
    with Session(engine) as session:
        yield session


def build_database():
    SQLModel.metadata.create_all(engine)
