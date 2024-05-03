from sqlmodel import create_engine, Session, SQLModel

sqlite_file_name = "rapid_db.db"
sqlite_url = f"sqlite:///{sqlite_file_name}"
engine = create_engine(sqlite_url, echo=True)


def get_session():
    with Session(engine) as session:
        yield session


def build_database():
    SQLModel.metadata.create_all(engine)
    