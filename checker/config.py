"""
config.py - Configuration Management
=====================================
This module handles loading and validating configuration from environment variables.
It reads settings from a .env file and makes them available to the rest of the application.

Environment Variables Used:
---------------------------
- SIDSP_BASE_URL      : (Required) The base URL of the SIDSP API (e.g., "https://api.example.com")
- SIDSP_TOKEN         : (Optional) A pre-generated API token. If provided, skips login.
- SIDSP_AUTH_EMAIL    : (Optional) Email for programmatic login (used if no token provided)
- SIDSP_AUTH_PASSWORD : (Optional) Password for programmatic login (used if no token provided)
- SIDSP_TIMEOUT_SEC   : (Optional) Request timeout in seconds (default: 20)
- SIDSP_CHECK_YEAR    : (Optional) Academic year to check applications for (default: "2025")
- SIDSP_EXCEL_HEADER_ROW : (Optional) Which row contains headers in Excel files (default: 0)

Example .env file:
------------------
SIDSP_BASE_URL=https://sidsp-api.example.com
SIDSP_TOKEN=eyJhbGciOiJIUzI1NiIs...
SIDSP_CHECK_YEAR=2025
"""

from dataclasses import dataclass
import os
from pathlib import Path
from dotenv import load_dotenv


# =============================================================================
# SETTINGS DATACLASS
# =============================================================================
# This dataclass holds all configuration values used throughout the application.
# Using a dataclass provides:
#   - Clean attribute access (settings.base_url instead of settings['base_url'])
#   - Type hints for IDE autocomplete
#   - Default values for optional settings

@dataclass
class Settings:
    """Container for all application configuration values."""
    
    # Required: The base URL of the API (e.g., "https://api.example.com")
    base_url: str
    
    # Optional: Pre-generated token (if provided, skips the login step)
    token: str | None = None
    
    # Optional: Credentials for programmatic login (used when token is not provided)
    auth_email: str | None = None
    auth_password: str | None = None
    
    # Optional: How long to wait for API responses before timing out
    timeout_sec: int = 20
    
    # Optional: Which academic year to check applications for
    # This is passed to the API when searching by TRN + Year
    check_year: str = "2025"
    
    # Optional: Which row in Excel files contains the column headers
    # 0 = first row (most common), 1 = second row, etc.
    excel_header_row: int = 0


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def _clean(v: str | None) -> str | None:
    """
    Clean and normalize an environment variable value.
    
    This handles common issues with .env files:
    - Extra whitespace around values
    - Values wrapped in quotes (single or double)
    - Empty strings that should be treated as None
    
    Examples:
        _clean('  hello  ')     -> 'hello'
        _clean('"quoted"')      -> 'quoted'
        _clean("'quoted'")      -> 'quoted'
        _clean('')              -> None
        _clean(None)            -> None
    """
    if v is None:
        return None
    
    # Remove leading/trailing whitespace
    v = v.strip()
    
    # Remove surrounding quotes if present
    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        v = v[1:-1]
    
    # Return None for empty strings
    return v if v else None


# =============================================================================
# MAIN CONFIGURATION LOADER
# =============================================================================

def load_settings() -> Settings:
    """
    Load application configuration from environment variables.
    
    This function:
    1. Finds and loads the .env file from the project root
    2. Reads all SIDSP_* environment variables
    3. Cleans and validates the values
    4. Returns a Settings object with all configuration
    
    Returns:
        Settings: A dataclass containing all configuration values
        
    Raises:
        RuntimeError: If SIDSP_BASE_URL is not set (it's required)
    
    Usage:
        settings = load_settings()
        print(settings.base_url)      # "https://api.example.com"
        print(settings.check_year)    # "2025"
    """
    # ---------------------------------------------------------------------
    # STEP 1: Load the .env file
    # ---------------------------------------------------------------------
    # The .env file should be in the project root (one level up from checker/)
    # Path(__file__) = this file (config.py)
    # .resolve() = get absolute path
    # .parents[1] = go up two levels (config.py -> checker/ -> project root)
    root_env = Path(__file__).resolve().parents[1] / ".env"
    
    # load_dotenv reads the .env file and adds variables to os.environ
    load_dotenv(dotenv_path=root_env)

    # ---------------------------------------------------------------------
    # STEP 2: Read and validate the base URL (required)
    # ---------------------------------------------------------------------
    base = _clean(os.getenv("SIDSP_BASE_URL"))
    
    if not base:
        raise RuntimeError(
            "SIDSP_BASE_URL is not set in environment. "
            "Please add it to your .env file."
        )
    
    # Ensure the URL has a scheme (http:// or https://)
    # If someone just puts "api.example.com", we add https://
    if not base.startswith("http"):
        base = "https://" + base
    
    # Remove trailing slash to make URL joining predictable
    # This way we can always do: base_url + "/api/v1/endpoint"
    base = base.rstrip("/")

    # ---------------------------------------------------------------------
    # STEP 3: Build and return the Settings object
    # ---------------------------------------------------------------------
    return Settings(
        base_url=base,
        
        # Authentication options (token OR email+password)
        token=_clean(os.getenv("SIDSP_TOKEN")),
        auth_email=_clean(os.getenv("SIDSP_AUTH_EMAIL")),
        auth_password=_clean(os.getenv("SIDSP_AUTH_PASSWORD")),
        
        # Request timeout (default: 20 seconds)
        timeout_sec=int(os.getenv("SIDSP_TIMEOUT_SEC", "20")),
        
        # Year to check applications for (default: 2025)
        check_year=_clean(os.getenv("SIDSP_CHECK_YEAR")) or "2025",
        
        # Excel header row (default: 0 = first row)
        excel_header_row=int(os.getenv("SIDSP_EXCEL_HEADER_ROW", "0")),
    )
