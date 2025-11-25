"""
selector.py - API Endpoint Selection
=====================================
This module determines which API endpoint(s) to call for checking if an
application exists in the SIDSP system.

Current Strategy:
-----------------
Use the ByYearTRN endpoint to search for applications by:
- TRN (Tax Registration Number) - the customer's unique identifier
- Year - the academic year to check (e.g., 2025)

This is the most accurate way to find applications because:
- The same TRN might have applications across multiple years
- We specifically want to check if they have an application for THIS year (2025)
"""

from typing import List, Tuple, Dict, Any


# =============================================================================
# TYPE DEFINITIONS
# =============================================================================

# An endpoint sequence is a list of (path, params) tuples
# Each tuple contains:
#   - path: The API endpoint path (e.g., "/api/v2/Applications/ByYearTRN")
#   - params: A dictionary of query parameters (e.g., {"trn": "123", "year": "2025"})
EndpointSequence = List[Tuple[str, Dict[str, str]]]


# =============================================================================
# ENDPOINT SELECTOR
# =============================================================================

def select_endpoint_sequence(
    row: Dict[str, Any], 
    default_year: str = "2025"
) -> EndpointSequence:
    """
    Determine which API endpoint(s) to call for checking a given row.
    
    This function looks at the data in the row and decides:
    1. What endpoint to call
    2. What parameters to pass
    
    Currently uses a simple strategy:
    - Call the ByYearTRN endpoint with the customer's TRN and the year
    
    Args:
        row: A dictionary containing the row data from the input file.
             Expected to have 'CUSTOMER_TRN' at minimum.
        default_year: The academic year to check (from settings, typically "2025")
    
    Returns:
        A list of (endpoint_path, parameters) tuples.
        Returns empty list if no valid TRN is found.
        
    Example:
        row = {'CUSTOMER_TRN': '100379893', 'REGISTRATION_NO': 'SLB-156439'}
        sequence = select_endpoint_sequence(row, "2025")
        # Returns: [("/api/v2/Applications/ByYearTRN", {"trn": "100379893", "year": "2025"})]
    """
    
    # -------------------------------------------------------------------------
    # STEP 1: Extract and validate the TRN
    # -------------------------------------------------------------------------
    # Get the CUSTOMER_TRN from the row, defaulting to empty string if not found
    # Convert to string and strip whitespace
    customer_trn = str(row.get('CUSTOMER_TRN') or '').strip()
    
    # If there's no TRN, we can't make any API calls
    if not customer_trn:
        return []
    
    # -------------------------------------------------------------------------
    # STEP 2: Determine the year to use
    # -------------------------------------------------------------------------
    # Always use the default year from settings (typically 2025)
    # This ensures consistent checking regardless of what's in the input file
    year = default_year
    
    # -------------------------------------------------------------------------
    # STEP 3: Build the endpoint sequence
    # -------------------------------------------------------------------------
    # We only use the ByYearTRN endpoint because:
    # - It's the most accurate way to find applications for a specific year
    # - The TRN might have applications in other years we don't care about
    
    sequence = [
        (
            "/api/v2/Applications/ByYearTRN",  # API endpoint path
            {"trn": customer_trn, "year": year}  # Query parameters
        )
    ]
    
    return sequence
