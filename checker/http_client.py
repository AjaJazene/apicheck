"""
http_client.py - HTTP Client for API Communication
===================================================
This module handles all HTTP communication with the SIDSP API, including:
- Authentication (login to get a token, or use a pre-existing token)
- Making API requests with automatic retry on transient failures
- Managing the HTTP session and headers

Features:
---------
- Automatic retry on server errors (500, 502, 503, 504) and timeouts
- Exponential backoff between retries
- Bearer token authentication
- Configurable timeouts
"""

import requests
import time
from requests.auth import HTTPBasicAuth
from .config import Settings


# =============================================================================
# RETRY CONFIGURATION
# =============================================================================
# These settings control how the client handles transient failures

# Which HTTP status codes should trigger a retry
# 408 = Request Timeout
# 500 = Internal Server Error
# 502 = Bad Gateway
# 503 = Service Unavailable
# 504 = Gateway Timeout
RETRY_STATUSES = (408, 500, 502, 503, 504)

# How many times to retry before giving up
# Total attempts = 1 initial + MAX_RETRIES
MAX_RETRIES = 2

# How long to wait between retries (exponential backoff)
# Wait time = BACKOFF_FACTOR * (2 ** attempt_number)
# Attempt 0: 0.5 * 1 = 0.5 seconds
# Attempt 1: 0.5 * 2 = 1.0 seconds
# Attempt 2: 0.5 * 4 = 2.0 seconds
BACKOFF_FACTOR = 0.5


# =============================================================================
# HTTP CLIENT CLASS
# =============================================================================

class HttpClient:
    """
    HTTP client for communicating with the SIDSP API.
    
    This client handles:
    - Authentication (either via login or static token)
    - Making GET requests to API endpoints
    - Automatic retry on transient failures
    - Session management
    
    Usage:
        # Create client
        client = HttpClient(settings)
        
        # Authenticate (one of these)
        client.set_static_token("your-token-here")  # If you have a token
        client.login_and_set_token()                # To login and get a token
        
        # Make API calls
        status, content_type, body = client.get_json("/api/v2/Applications/ByYearTRN", {"trn": "123", "year": "2025"})
        
        # Clean up
        client.close()
    """
    
    def __init__(self, settings: Settings):
        """
        Initialize the HTTP client.
        
        Args:
            settings: Configuration object containing base URL, timeout, and auth settings
        """
        self.settings = settings
        
        # Create a requests Session for connection pooling and cookie persistence
        # Sessions are more efficient than making individual requests
        self.s = requests.Session()
        
        # Store commonly used settings for convenience
        self.base = settings.base_url      # e.g., "https://api.example.com"
        self.timeout = settings.timeout_sec # e.g., 20 seconds

    # -------------------------------------------------------------------------
    # AUTHENTICATION METHODS
    # -------------------------------------------------------------------------

    def _set_auth_header(self, token: str):
        """
        Set the Bearer token authorization header for all future requests.
        
        This is called after successful authentication to ensure all
        subsequent API calls include the authorization token.
        
        Args:
            token: The JWT or access token to use for authorization
        """
        self.s.headers.update({
            "Authorization": f"Bearer {token}",  # Standard Bearer token format
            "Accept": "application/json"          # We expect JSON responses
        })

    def _extract_token(self, data: dict) -> str | None:
        """
        Extract the authentication token from an API response.
        
        Different APIs return tokens in different structures:
        - {"user": {"token": "..."}}    - Nested under 'user'
        - {"token": "..."}              - Direct 'token' key
        - {"accessToken": "..."}        - camelCase 'accessToken'
        - {"access_token": "..."}       - snake_case 'access_token'
        
        This method tries each structure in order and returns the first token found.
        
        Args:
            data: The JSON response from the authentication endpoint
            
        Returns:
            The token string if found, None otherwise
        """
        # Try nested structure first: {"user": {"token": "..."}}
        if "user" in data and isinstance(data["user"], dict):
            if "token" in data["user"]:
                return data["user"]["token"]
        
        # Try flat structures
        if "token" in data:
            return data["token"]
        if "accessToken" in data:
            return data["accessToken"]
        if "access_token" in data:
            return data["access_token"]
        
        # No token found
        return None

    def login_and_set_token(self):
        """
        Perform programmatic login to get an authentication token.
        
        This sends a POST request to the Auth endpoint with email/password
        credentials. On success, the token is extracted from the response
        and set for all future requests.
        
        Raises:
            RuntimeError: If credentials are missing, login fails, or no token is returned
        """
        # Build the authentication URL
        url = f"{self.base}/api/v1/Auth"
        
        # Get credentials from settings
        email = self.settings.auth_email
        pwd = self.settings.auth_password

        # Validate credentials are provided
        if not (email and pwd):
            raise RuntimeError(
                "Missing SIDSP_AUTH_EMAIL or SIDSP_AUTH_PASSWORD. "
                "Set these in .env or provide SIDSP_TOKEN instead."
            )

        # Prepare the request
        # We send credentials both in the JSON body and as Basic Auth headers
        # (some APIs accept one or the other)
        payload = {"email": email, "password": pwd}
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        
        # Make the login request
        r = self.s.post(
            url, 
            auth=HTTPBasicAuth(email, pwd),  # Basic Auth header
            json=payload,                     # JSON body with credentials
            headers=headers, 
            timeout=self.timeout
        )

        # Check for expected 201 Created status
        # Some APIs return 200 OK, but this one expects 201
        if r.status_code != 201:
            raise RuntimeError(
                f"Authentication failed with status {r.status_code}. "
                f"Response: {r.text[:300]}"
            )

        # Verify we got JSON back
        content_type = r.headers.get("content-type", "").lower()
        if "application/json" not in content_type:
            raise RuntimeError(
                f"Expected JSON response but got {content_type}. "
                f"Body: {r.text[:200]}"
            )
        
        # Parse the response and extract the token
        data = r.json()
        token = self._extract_token(data)
        
        if not token:
            raise RuntimeError(
                f"Authentication succeeded (201) but no token found in response. "
                f"Response keys: {list(data.keys())}"
            )
        
        # Set the token for all future requests
        self._set_auth_header(token)

    def set_static_token(self, token: str):
        """
        Use a pre-existing token instead of logging in.
        
        This is useful when you already have a valid token (e.g., from
        the SIDSP_TOKEN environment variable) and don't need to login.
        
        Args:
            token: The JWT or access token to use
        """
        self._set_auth_header(token)

    # -------------------------------------------------------------------------
    # API REQUEST METHODS
    # -------------------------------------------------------------------------
    
    def get_json(self, path: str, params: dict | None = None):
        """
        Make a GET request to an API endpoint with automatic retry.
        
        This method:
        1. Builds the full URL from base + path
        2. Makes the GET request with optional query parameters
        3. Retries on transient failures (server errors, network issues)
        4. Returns the response details
        
        Args:
            path: The API endpoint path (e.g., "/api/v2/Applications/ByYearTRN")
            params: Optional query parameters (e.g., {"trn": "123", "year": "2025"})
            
        Returns:
            A tuple of (status_code, content_type, body):
            - status_code: HTTP status code (e.g., 200, 404, 500)
            - content_type: The Content-Type header value
            - body: The response body as a string
            
        Example:
            status, content_type, body = client.get_json(
                "/api/v2/Applications/ByYearTRN",
                {"trn": "100379893", "year": "2025"}
            )
            if status == 200:
                data = json.loads(body)
                print(f"Found {len(data)} applications")
        """
        # Build the full URL
        url = f"{self.base}{path}"
        
        # Retry loop
        for attempt in range(MAX_RETRIES + 1):
            try:
                # Make the GET request
                r = self.s.get(url, params=params, timeout=self.timeout)
                
                # Check if we should retry (server error and not last attempt)
                if r.status_code in RETRY_STATUSES and attempt < MAX_RETRIES:
                    # Calculate wait time with exponential backoff
                    wait_time = BACKOFF_FACTOR * (2 ** attempt)
                    print(
                        f"[{r.status_code}] Retrying {path} in {wait_time:.2f}s "
                        f"(Attempt {attempt + 1}/{MAX_RETRIES})..."
                    )
                    time.sleep(wait_time)
                    continue  # Try again
                
                # Return the result (success or non-retryable error)
                return (
                    r.status_code, 
                    r.headers.get("content-type", ""), 
                    r.text or ""  # Return full response body
                )

            except requests.RequestException as e:
                # Network errors: timeout, connection refused, DNS failure, etc.
                if attempt < MAX_RETRIES:
                    wait_time = BACKOFF_FACTOR * (2 ** attempt)
                    print(
                        f"[Network Error] Retrying {path} in {wait_time:.2f}s "
                        f"(Attempt {attempt + 1}/{MAX_RETRIES})..."
                    )
                    time.sleep(wait_time)
                    continue  # Try again
                
                # Final failure after all retries
                return 0, "", f"Network error: {type(e).__name__}: {e}"

        # Should never reach here, but just in case
        return 0, "", "Max retries exceeded unexpectedly"
    
    # -------------------------------------------------------------------------
    # CLEANUP METHODS
    # -------------------------------------------------------------------------
    
    def close(self):
        """
        Close the HTTP session and release resources.
        
        Always call this when you're done with the client to properly
        close connections and free up resources.
        """
        self.s.close()
