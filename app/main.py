import logging

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse

from api import metadata
from core.config import (
    CORS_ORIGINS,
    CURRENT_API_VERSION,
    DOCS_URL,
    OPENAPI_URL,
    SERVICE_DESCRIPTION,
    SERVICE_ID,
    SERVICE_NAME,
)
from scripts import LoadMeta

logger = logging.getLogger(f"{SERVICE_ID}-app")

app = FastAPI(
    title=SERVICE_NAME,
    openapi_url=OPENAPI_URL,
    docs_url=DOCS_URL,
    redoc_url=None,
    version=CURRENT_API_VERSION,
    description=SERVICE_DESCRIPTION,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", include_in_schema=False)
def home():
    return RedirectResponse(url=f"/{SERVICE_ID}")


@app.on_event("startup")
def startup_event():
    LoadMeta()


app.include_router(
    metadata.router, prefix=f"/{SERVICE_ID}", tags=[f"{SERVICE_ID}"]
)
