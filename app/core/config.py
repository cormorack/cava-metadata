import os
import fsspec

from pydantic import BaseSettings


class Settings(BaseSettings):
    """ Setting for the whole application """
    # TODO: Switch over fully to this settings
    SERVICE_NAME = "Metadata Service"
    SERVICE_ID = "metadata"
    OPENAPI_URL = f"/{SERVICE_ID}/openapi.json"
    DOCS_URL = f"/{SERVICE_ID}/"
    SERVICE_DESCRIPTION = """Metadata service for Interactive Oceans."""

    CORS_ORIGINS = [
        "http://localhost",
        "http://localhost:8000",
        "http://localhost:5000",
        "http://localhost:4000",
        "https://appdev.ooica.net",
        "https://app-dev.ooica.net",
        "https://app.interactiveoceans.washington.edu",
        "https://api-dev.ooica.net",
        "https://api.interactiveoceans.washington.edu",
    ]

    BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # API VERSION
    CURRENT_API_VERSION = 2.0

    # Cloud Credentials
    AWS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", None)
    AWS_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", None)

    # Redis configurations
    REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
    REDIS_PORT = os.environ.get("REDIS_PORT", 6379)

    # OOI Configurations
    BASE_URL = "https://ooinet.oceanobservatories.org"
    M2M_URL = "api/m2m"
    USERNAME = os.environ.get("OOI_USERNAME", "")
    TOKEN = os.environ.get("OOI_TOKEN", "")

    # File Systems Configurations
    FILE_SYSTEMS = {
        "minio_s3": fsspec.filesystem(
            "s3", client_kwargs={"endpoint_url": "http://minio:9000"}
        ),
        "aws_s3": fsspec.filesystem(
            "s3",
            skip_instance_cache=True,
            use_listings_cache=False,
            config_kwargs={"max_pool_connections": 1000},
        ),
    }
    GOOGLE_SERVICE_JSON = os.environ.get("GOOGLE_SERVICE_JSON", "",)
    DATA_BUCKET = 'ooi-data'

    # Data sources
    METADATA_SOURCE = "s3://ooi-metadata"
    METADATA_BUCKET = "ooi-metadata"


settings = Settings()

# API SETTINGS
SERVICE_NAME = "Metadata Service"
SERVICE_ID = "metadata"
OPENAPI_URL = f"/{SERVICE_ID}/openapi.json"
DOCS_URL = f"/{SERVICE_ID}/"
SERVICE_DESCRIPTION = """Metadata service for Interactive Oceans."""

CORS_ORIGINS = [
    "http://localhost",
    "http://localhost:8000",
    "http://localhost:5000",
    "http://localhost:4000",
    "https://appdev.ooica.net",
    "https://app-dev.ooica.net",
    "https://app.interactiveoceans.washington.edu",
    "https://api-dev.ooica.net",
    "https://api.interactiveoceans.washington.edu",
    "https://api-development.ooica.net",
    "https://cava-portal.netlify.app",
]

BASE_PATH = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# API VERSION
CURRENT_API_VERSION = 2.0

# Cloud Credentials
AWS_KEY = os.environ.get("AWS_ACCESS_KEY_ID", None)
AWS_SECRET = os.environ.get("AWS_SECRET_ACCESS_KEY", None)

# Redis configurations
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = os.environ.get("REDIS_PORT", 6379)

# OOI Configurations
BASE_URL = "https://ooinet.oceanobservatories.org"
M2M_URL = "api/m2m"
USERNAME = os.environ.get("OOI_USERNAME", None)
TOKEN = os.environ.get("OOI_TOKEN", None)

# File Systems Configurations
FILE_SYSTEMS = {
    "minio_s3": fsspec.filesystem(
        "s3", client_kwargs={"endpoint_url": "http://minio:9000"}
    ),
    "aws_s3": fsspec.filesystem(
        "s3",
        skip_instance_cache=True,
        use_listings_cache=False,
        config_kwargs={"max_pool_connections": 1000},
    ),
}
GOOGLE_SERVICE_JSON = os.environ.get("GOOGLE_SERVICE_JSON", "",)
DATA_BUCKET = 'ooi-data'

# Data sources
METADATA_SOURCE = "s3://ooi-metadata"
