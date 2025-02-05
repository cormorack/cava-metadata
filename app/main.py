import logging

from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import RedirectResponse

from prometheus_fastapi_instrumentator import Instrumentator

from api import metadata
from core.config import settings

from scripts import LoadMeta, load_instrument_catalog

logger = logging.getLogger(f"{settings.SERVICE_ID}-app")

app = FastAPI(
    title=settings.SERVICE_NAME,
    openapi_url=settings.OPENAPI_URL,
    docs_url=settings.DOCS_URL,
    redoc_url=None,
    version=settings.CURRENT_API_VERSION,
    description=settings.SERVICE_DESCRIPTION,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    # Regex for dev in netlify
    allow_origin_regex='https://.*cava-portal\.netlify\.app',
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", include_in_schema=False)
def home():
    return RedirectResponse(url=f"/{settings.SERVICE_ID}")


@app.on_event("startup")
def startup_event():
    LoadMeta()
    load_instrument_catalog()


app.include_router(
    metadata.router, prefix=f"/{settings.SERVICE_ID}", tags=[f"{settings.SERVICE_ID}"]
)

# Prometheus instrumentation
Instrumentator().instrument(app).expose(
    app, endpoint="/metadata/metrics", include_in_schema=False
)
