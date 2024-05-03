import pytest
from sqlmodel import Session, SQLModel, create_engine
from rapid.crud.catalog import source_crud
from rapid.models.catalog import Source
from pydantic import ValidationError


@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine(
        "sqlite://"
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


valid_source = Source.Create(name="1", description="1", connection_details="1")


def test_create_source(session: Session):
    created_source = source_crud.create_model(session, valid_source)

    assert created_source.id == 1
    assert created_source.name == "1"
    assert created_source.description == "1"
    assert created_source.connection_details == "1"


def test_create_invalid_source():
    with pytest.raises(ValidationError) as exception_info:
        _ = Source.Create(name=1, description=True, connection_details=1.0)

    assert len(exception_info.value.errors()) == 3


def test_get_source(session: Session):
    _ = source_crud.create_model(session, valid_source)

    db_source = source_crud.get_model_on_id(session, 1)

    assert db_source.id == 1
    assert db_source.name == "1"
    assert db_source.description == "1"
    assert db_source.connection_details == "1"


def test_get_source_invalid(session: Session):
    with pytest.raises(ValueError) as exception_info:
        source_crud.get_model_on_id(session, model_id=1)

    assert str(exception_info.value) == f"404: source with ID: 1 not found."


def test_get_sources(session: Session):
    source1 = Source.Create(name="2", description="2", connection_details="2")

    _ = source_crud.create_model(session, valid_source)
    _ = source_crud.create_model(session, source1)

    db_sources = source_crud.get_all_models(session)
    assert len(db_sources) == 2
    assert db_sources[0].id == 1
    assert db_sources[1].id == 2


def test_get_sources_invalid(session: Session):
    with pytest.raises(ValueError) as exception_info:
        source_crud.get_all_models(session)

    assert str(exception_info.value) == f"404: no sources found."


def test_update_source(session: Session):
    db_source = source_crud.create_model(session, valid_source)

    source_update = Source.Update(name="2", description="2", connection_details="2")
    db_changed_source = source_crud.update_model(session, db_source.id, source_update)

    assert db_changed_source.name == "2"
    assert db_changed_source.description == "2"
    assert db_changed_source.connection_details == "2"

    db_changed_source_from_db = source_crud.get_model_on_id(session, model_id=db_source.id)

    assert db_changed_source_from_db.name == "2"
    assert db_changed_source_from_db.description == "2"
    assert db_changed_source_from_db.connection_details == "2"


def test_update_invalid(session: Session):
    _ = source_crud.create_model(session, valid_source)

    with pytest.raises(ValidationError) as exception_info:
        _ = Source.Update(name=1, description=True, connection_details=1.0)

    assert len(exception_info.value.errors()) == 3


def test_soft_delete_source(session: Session):
    source_crud.create_model(session, valid_source)

    db_source = source_crud.get_model_on_id(session, model_id=1)
    assert db_source.is_active

    source_crud.delete_model(session, model_id=1)
    db_deleted_source = source_crud.get_model_on_id(session, model_id=1)

    assert not db_deleted_source.is_active


def test_hard_delete_source(session: Session):
    _ = source_crud.create_model(session, valid_source)

    db_source = source_crud.get_model_on_id(session, model_id=1)
    assert db_source.is_active

    source_crud.delete_model(session, model_id=1, hard_delete=True)

    with pytest.raises(ValueError) as exception_info:
        source_crud.get_model_on_id(session, 1)

    assert str(exception_info.value) == f"404: source with ID: 1 not found."


def test_delete_invalid(session):
    with pytest.raises(ValueError) as exception_info:
        source_crud.delete_model(session, 1)

    assert str(exception_info.value) == f"404: source with ID: 1 not found."