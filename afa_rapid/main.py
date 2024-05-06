from contextlib import asynccontextmanager
from fastapi import FastAPI, Response
from .rapid_logging.router import router as rapid_logging_router
from .metadata.router import router as metadata_router
from .orchestration.router import router as orchestration_router
from boilerplate.rapid_db.database import build_database


@asynccontextmanager
async def lifespan(app: FastAPI):
    build_database()
    yield


app = FastAPI(lifespan=lifespan)


app.include_router(metadata_router)
app.include_router(rapid_logging_router)
app.include_router(orchestration_router)


@app.get("/", include_in_schema=False)
def read_root():
    return Response("Server is running")
