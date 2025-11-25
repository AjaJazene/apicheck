import requests
import time
from requests.auth import HTTPBasicAuth
from .config import Settings

# Configuration for retry logic
RETRY_STATUSES = (408, 500, 502, 503, 504)
MAX_RETRIES = 2  # Total attempts = 1 initial + 2 retries
BACKOFF_FACTOR = 0.5  # Wait time = 0.5 * (2 ** attempt)

class HttpClient:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.s = requests.Session()
        self.base = settings.base_url
        self.timeout = settings.timeout_sec

    def _set_auth_header(self, token: str):
        """Set Bearer token authorization header."""
        self.s.headers.update({
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        })

    def _extract_token(self, data: dict) -> str | None:
        """
        Extract token from API response with clear precedence.
        Supports multiple response structures.
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
        
        return None

    def login_and_set_token(self):
        """
        Performs programmatic login using Basic Auth.
        Expects 201 Created status with token in response.
        """
        url = f"{self.base}/api/v1/Auth"
        email = self.settings.auth_email
        pwd = self.settings.auth_password

        if not (email and pwd):
            raise RuntimeError(
                "Missing SIDSP_AUTH_EMAIL or SIDSP_AUTH_PASSWORD. "
                "Set these in .env or provide SIDSP_TOKEN instead."
            )

        # Primary attempt: JSON body + Basic Auth
        payload = {"email": email, "password": pwd}
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        
        r = self.s.post(
            url, 
            auth=HTTPBasicAuth(email, pwd), 
            json=payload,
            headers=headers, 
            timeout=self.timeout
        )

        # Check for expected 201 Created status
        if r.status_code != 201:
            raise RuntimeError(
                f"Authentication failed with status {r.status_code}. "
                f"Response: {r.text[:300]}"
            )

        # Parse response
        content_type = r.headers.get("content-type", "").lower()
        if "application/json" not in content_type:
            raise RuntimeError(
                f"Expected JSON response but got {content_type}. "
                f"Body: {r.text[:200]}"
            )
        
        data = r.json()
        token = self._extract_token(data)
        
        if not token:
            raise RuntimeError(
                f"Authentication succeeded (201) but no token found in response. "
                f"Response keys: {list(data.keys())}"
            )
        
        self._set_auth_header(token)

    def set_static_token(self, token: str):
        """Use a pre-existing token from environment."""
        self._set_auth_header(token)
    
    def get_json(self, path: str, params: dict | None = None):
        """
        Make GET request with automatic retry on transient failures.
        Returns: (status_code, content_type, body_snippet)
        """
        url = f"{self.base}{path}"
        
        for attempt in range(MAX_RETRIES + 1):
            try:
                r = self.s.get(url, params=params, timeout=self.timeout)
                
                # Retry on server errors
                if r.status_code in RETRY_STATUSES and attempt < MAX_RETRIES:
                    wait_time = BACKOFF_FACTOR * (2 ** attempt)
                    print(
                        f"[{r.status_code}] Retrying {path} in {wait_time:.2f}s "
                        f"(Attempt {attempt + 1}/{MAX_RETRIES})..."
                    )
                    time.sleep(wait_time)
                    continue
                
                # Return result (success or non-retryable error)
                return (
                    r.status_code, 
                    r.headers.get("content-type", ""), 
                    r.text or ""  # Return FULL text, not truncated
                )

            except requests.RequestException as e:
                # Network errors (timeout, connection, DNS)
                if attempt < MAX_RETRIES:
                    wait_time = BACKOFF_FACTOR * (2 ** attempt)
                    print(
                        f"[Network Error] Retrying {path} in {wait_time:.2f}s "
                        f"(Attempt {attempt + 1}/{MAX_RETRIES})..."
                    )
                    time.sleep(wait_time)
                    continue
                
                # Final failure
                return 0, "", f"Network error: {type(e).__name__}: {e}"

        # Should never reach here
        return 0, "", "Max retries exceeded unexpectedly"
    
    def close(self):
        """Close the underlying requests session."""
        self.s.close()