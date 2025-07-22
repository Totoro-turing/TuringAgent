import os
from typing import Any, Dict, Optional

# Import dotenv if available, but don't require it
try:
    from dotenv import load_dotenv
    # Load .env file if it exists
    load_dotenv()
    print("Successfully loaded dotenv")
except ImportError:
    print("WARNING: python-dotenv not found, environment variables must be set manually")
    # We'll just rely on OS environment variables being set manually

from pydantic import field_validator
from pydantic_settings import BaseSettings

# Version
VERSION = "0.1.0"


class Settings(BaseSettings):
    """Base settings for the application."""
    # Databricks API configuration
    DATABRICKS_HOST: str = os.environ.get("DATABRICKS_HOST", "https://example.databricks.net")
    DATABRICKS_TOKEN: str = os.environ.get("DATABRICKS_TOKEN", "dapi_token_placeholder")
    DATABRICKS_HTTP: str = os.environ.get("DATABRICKS_HTTP", "")
    AZURE_USERNAME: str = os.environ.get("AZURE_USERNAME", "")
    AZURE_PASSWORD: str = os.environ.get("AZURE_PASSWORD", "")
    DEEPSEEK_API_URL: str = os.environ.get("DEEPSEEK_API_URL", "https://api.deepseek.com")
    DEEPSEEK_API_TOKEN: str = os.environ.get("DEEPSEEK_API_TOKEN", "")
    DATABRICKS_CLUSTER_ID: str = os.environ.get("DATABRICKS_CLUSTER_ID", "")
    # Server configuration
    SERVER_HOST: str = os.environ.get("SERVER_HOST", "0.0.0.0") 
    SERVER_PORT: int = int(os.environ.get("SERVER_PORT", "8000"))
    DEBUG: bool = os.environ.get("DEBUG", "False").lower() == "true"

    LOCAL_REPO_PATH: str = os.environ.get("LOCAL_REPO_PATH", "")
    EMAIL_TOKEN: str = os.environ.get("EMAIL_TOKEN", "")
    # Logging
    LOG_LEVEL: str = os.environ.get("LOG_LEVEL", "INFO")
    
    # Version
    VERSION: str = VERSION

    @field_validator("DATABRICKS_HOST")
    def validate_databricks_host(cls, v: str) -> str:
        """Validate Databricks host URL."""
        if not v.startswith(("https://", "http://")):
            raise ValueError("DATABRICKS_HOST must start with http:// or https://")
        return v

    class Config:
        """Pydantic configuration."""

        env_file = ".env"
        case_sensitive = True
        extra = "allow"  # 允许额外的字段


# Create global settings instance
settings = Settings()


def get_api_headers() -> Dict[str, str]:
    """Get headers for Databricks API requests."""
    return {
        "Authorization": f"Bearer {settings.DATABRICKS_TOKEN}",
        "Content-Type": "application/json",
    }


def get_databricks_api_url(endpoint: str) -> str:
    """
    Construct the full Databricks API URL.
    
    Args:
        endpoint: The API endpoint path, e.g., "/api/2.0/clusters/list"
    
    Returns:
        Full URL to the Databricks API endpoint
    """
    # Ensure endpoint starts with a slash
    if not endpoint.startswith("/"):
        endpoint = f"/{endpoint}"

    # Remove trailing slash from host if present
    host = settings.DATABRICKS_HOST.rstrip("/")
    
    return f"{host}{endpoint}" 