import os

from typing import List, Dict, Optional, ClassVar
from pydantic import RedisDsn
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Setting for the whole application"""

    SERVICE_NAME: ClassVar[str] = "Metadata Service"
    SERVICE_ID: ClassVar[str] = "metadata"
    OPENAPI_URL: ClassVar[str] = f"/{SERVICE_ID}/openapi.json"
    DOCS_URL: ClassVar[str] = f"/{SERVICE_ID}/"
    SERVICE_DESCRIPTION: ClassVar[str] = """Metadata service for Interactive Oceans."""

    CORS_ORIGINS: List[str] = [
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

    BASE_PATH: str = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    # API VERSION
    CURRENT_API_VERSION: str = '2.0'

    # Cloud Credentials
    AWS_KEY: Optional[str] = os.environ.get("AWS_ACCESS_KEY_ID", None)
    AWS_SECRET: Optional[str] = os.environ.get("AWS_SECRET_ACCESS_KEY", None)

    # Redis configurations
    REDIS_URI: RedisDsn = os.environ.get(
        "REDIS_URI", "redis://localhost:6379/0"
    )

    # OOI Configurations
    BASE_URL: str = "https://ooinet.oceanobservatories.org"
    M2M_URL: str = "api/m2m"
    USERNAME: str = os.environ.get("OOI_USERNAME", "")
    TOKEN: str = os.environ.get("OOI_TOKEN", "")

    # File Systems Configurations
    FILE_SYSTEMS: Dict = {
        "minio_s3": dict(
            protocol="s3", client_kwargs={"endpoint_url": "http://minio:9000"}
        ),
        "aws_s3": dict(
            protocol="s3",
            skip_instance_cache=True,
            use_listings_cache=False,
            config_kwargs={"max_pool_connections": 1000},
        ),
    }

    CAVA_ASSET_URL: str = "https://docs.google.com/spreadsheets/d/1YlZ6sKy11HMi64ZJOPkO76kMxpWOuZdTSiE8ODYR-Yc/gviz/tq?tqx=out:csv&sheet="

    DATA_BUCKET: str = 'ooi-data-prod'

    # Data sources
    METADATA_SOURCE: str = "s3://ooi-metadata-prod"
    METADATA_BUCKET: str = "ooi-metadata-prod"


settings = Settings()
