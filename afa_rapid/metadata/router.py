from fastapi import APIRouter, Depends
from .logic import parse_metadata
from boilerplate.rapid_db.database import get_session
from sqlmodel import Session


router = APIRouter(prefix="/metadata", tags=["metadata"])


@router.get("/ingest_metadata")
def ingest_metadata(session: Session = Depends(get_session)):
    parse_metadata(session)
