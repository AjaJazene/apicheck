from dataclasses import dataclass
import os
from pathlib import Path
from dotenv import load_dotenv

# Dataclass that holds runtime configuration values used across the app.
# Using dataclass for convenient construction and attribute access.
@dataclass
class Settings:
    base_url: str
    token: str | None = None
    auth_email: str | None = None
    auth_password: str | None = None
    timeout_sec: int = 20
    check_year: str = "2025"  # NEW: Configurable year
    excel_header_row: int = 0  # NEW: Configurable header row (0-indexed)

def _clean(v: str | None) -> str | None:
    """Normalize environment string values.

    - Trim surrounding whitespace.
    - Remove surrounding single or double quotes if present.
    - Return None if the resulting string is empty.
    This helps when values in a .env file are quoted or contain accidental spaces.
    """
    if v is None:
        return None
    v = v.strip()
    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        v = v[1:-1]
    return v if v else None

def load_settings() -> Settings:
    """Load configuration from environment variables and return a Settings object.

    Behavior:
    - Loads a .env file located at the repository root (one directory up from this module).
    - Reads required and optional SIDSP_* environment variables.
    - Cleans values (quotes/whitespace) using _clean().
    - Validates that a base URL is present and normalizes it (ensures scheme and strips trailing slash).
    - Converts numeric environment variables to int with sensible defaults.
    """
    # Determine the path to the repository root .env (module is in checker/, so go up one level)
    root_env = Path(__file__).resolve().parents[1] / ".env"
    load_dotenv(dotenv_path=root_env)  # load variables from that .env into os.environ

    # Read and normalize the base URL; _clean removes surrounding quotes/whitespace.
    base = _clean(os.getenv("SIDSP_BASE_URL"))
    if not base:
        # Missing required configuration: stop early with a clear error.
        raise RuntimeError("SIDSP_BASE_URL is not set in environment")
    # Ensure a scheme exists; if the value is a bare domain, prefix with https://
    if not base.startswith("http"):
        base = "https://" + base
    # Strip any trailing slash to make URL joining predictable
    base = base.rstrip("/")

    # Construct and return the Settings dataclass with typed and cleaned values.
    return Settings(
        base_url=base,
        token=_clean(os.getenv("SIDSP_TOKEN")),
        auth_email=_clean(os.getenv("SIDSP_AUTH_EMAIL")),
        auth_password=_clean(os.getenv("SIDSP_AUTH_PASSWORD")),
        timeout_sec=int(os.getenv("SIDSP_TIMEOUT_SEC", "20")),  # default 20 sec if not set
        check_year=_clean(os.getenv("SIDSP_CHECK_YEAR")) or "2025",  # fallback to "2025"
        excel_header_row=int(os.getenv("SIDSP_EXCEL_HEADER_ROW", "0")),  # default header row 0
    )
