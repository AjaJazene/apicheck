"""
run_checker.py - Main Application Entry Point
==============================================
This is the main script that orchestrates the application presence checking process.

What it does:
-------------
1. Loads configuration from environment variables (.env file)
2. Authenticates with the SIDSP API
3. Reads the input file (Excel or CSV)
4. For each row, calls the API to check if an application exists
5. Writes the results to a CSV file

Usage:
------
    python -m checker.run_checker input.xlsx
    python -m checker.run_checker data.csv --output-dir reports
    python -m checker.run_checker file.xlsx --dry-run

Command Line Options:
---------------------
    input_file      : Path to input Excel (.xlsx, .xls) or CSV file (required)
    --output-dir    : Directory for output CSV (default: "out")
    --dry-run       : Load and validate input without making API calls
    --debug         : Enable debug logging for troubleshooting
"""

import sys
import logging
import time
import csv
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
from urllib.parse import urlencode

# Import our modules
from .config import load_settings
from .http_client import HttpClient
from .loader import load_input_data
from .selector import select_endpoint_sequence


# =============================================================================
# CONFIGURATION CONSTANTS
# =============================================================================

# Logging level: INFO shows progress, DEBUG shows detailed API responses
LOG_LEVEL = logging.INFO

# Default output directory for result CSV files
OUTPUT_DIR = "out"

# Rate limiting: milliseconds to wait between API calls
# This prevents overwhelming the server and getting rate-limited
RATE_LIMIT_MS = 250  # 250ms = 4 requests per second max


# =============================================================================
# LOGGING SETUP
# =============================================================================

# Configure logging to show timestamps and log levels
logging.basicConfig(
    level=LOG_LEVEL, 
    format='%(asctime)s [%(levelname)s] %(message)s',  # e.g., "14:30:45 [INFO] Starting..."
    datefmt='%H:%M:%S'  # Time only, not full date
)

# Get a logger for this module
logger = logging.getLogger(__name__)


# =============================================================================
# CORE CHECKING FUNCTION
# =============================================================================

def check_row_status(
    client: HttpClient, 
    row: Dict[str, Any],
    default_year: str
) -> Dict[str, Any]:
    """
    Check if an application exists in the SIDSP system for a given row.
    
    This function:
    1. Determines which API endpoint to call based on the row data
    2. Makes the API call (with rate limiting)
    3. Interprets the response to determine if the application exists
    4. Returns a result dictionary with all the details
    
    Args:
        client: The HTTP client for making API calls
        row: A dictionary containing the row data from the input file
        default_year: The academic year to check (e.g., "2025")
        
    Returns:
        A dictionary containing:
        - InputRow: The row number from the input file
        - TRN: The customer's Tax Registration Number
        - ApplicationNumber: The registration number (e.g., SLB-156439)
        - YearChecked: The year that was checked
        - EndpointUsed: The full API URL that was called
        - HTTPStatus: The HTTP status code returned
        - Present: True if found, False if not found, None if error
        - Note: Human-readable explanation of the result
        - CheckedAt: Timestamp of when the check was performed
    """
    
    # -------------------------------------------------------------------------
    # STEP 1: Get the endpoint(s) to call
    # -------------------------------------------------------------------------
    sequence = select_endpoint_sequence(row, default_year)
    
    # -------------------------------------------------------------------------
    # STEP 2: Prepare the base result dictionary
    # -------------------------------------------------------------------------
    # This will be updated as we make API calls
    base_result = {
        "InputRow": row.get('InputRow'),                    # Row number from input file
        "TRN": row.get('CUSTOMER_TRN', ''),                 # Customer's TRN
        "ApplicationNumber": row.get('REGISTRATION_NO', ''),# Application number
        "YearChecked": default_year,                        # Year we're checking for
        "EndpointUsed": "NO_STRATEGY",                      # Will be updated with actual URL
        "HTTPStatus": 0,                                    # Will be updated with response code
        "Present": None,                                    # True/False/None
        "Note": "No valid identifier (CUSTOMER_TRN) found", # Human-readable note
        "CheckedAt": datetime.now().isoformat()             # When we checked
    }
    
    # If no endpoints to call (missing TRN), return early
    if not sequence:
        return base_result

    # -------------------------------------------------------------------------
    # STEP 3: Try each endpoint in the sequence
    # -------------------------------------------------------------------------
    for path, params in sequence:
        # Skip if no parameters (shouldn't happen, but safety check)
        if not params:
            continue
        
        # Rate limiting: wait before making the request
        # This prevents hitting the server too fast
        time.sleep(RATE_LIMIT_MS / 1000.0)  # Convert ms to seconds
        
        # Make the API call
        status, content_type, body = client.get_json(path, params=params)
        body_snippet = body[:200].strip()  # First 200 chars for error messages
        
        # Record which endpoint was used (for debugging/reporting)
        param_str = urlencode(params)
        base_result["EndpointUsed"] = f"{path}?{param_str}"
        base_result["HTTPStatus"] = status
        
        # ---------------------------------------------------------------------
        # Handle the response based on status code
        # ---------------------------------------------------------------------
        
        if status == 200:
            # 200 OK - Request succeeded, but we need to check if data exists
            try:
                import json
                data = json.loads(body)
                
                # Debug logging (only visible with --debug flag)
                logger.debug(f"Response data type: {type(data)}, value: {data}")
                
                # Check if the response is "empty" in various ways
                # The API might return 200 with an empty array/object meaning "not found"
                is_empty = False
                
                if data is None:
                    is_empty = True
                elif isinstance(data, list) and len(data) == 0:
                    # Empty array: []
                    is_empty = True
                elif isinstance(data, dict) and len(data) == 0:
                    # Empty object: {}
                    is_empty = True
                elif isinstance(data, dict):
                    # Object with empty arrays inside
                    # e.g., {"applications": []}
                    for value in data.values():
                        if isinstance(value, list) and len(value) == 0:
                            is_empty = True
                            break
                
                if is_empty:
                    # Empty response means not found, try next endpoint
                    logger.debug(f"200 but empty response on {path}, trying next...")
                    continue
                
                # Data found! Application exists
                base_result['Present'] = True
                base_result['Note'] = f"Present (200 OK with data)"
                logger.debug(f"Found data: {str(data)[:100]}")
                return base_result
                
            except (json.JSONDecodeError, ValueError) as e:
                # Couldn't parse the response as JSON - something's wrong
                base_result['Present'] = False
                base_result['Note'] = f"Error: Invalid JSON response - {str(e)[:50]}"
                return base_result
        
        elif status == 404:
            # 404 Not Found - Application doesn't exist at this endpoint
            # Continue to try the next endpoint (if any)
            logger.debug(f"404 on {path}, trying next endpoint...")
            continue
        
        else:
            # Other error (400, 401, 403, 500, etc.)
            # These are not retryable, so return immediately
            base_result['Present'] = False
            base_result['Note'] = f"Error ({status}): {body_snippet}"
            return base_result
    
    # -------------------------------------------------------------------------
    # STEP 4: All endpoints exhausted
    # -------------------------------------------------------------------------
    # If we get here, all endpoints returned 404 or empty responses
    if base_result.get("Present") is None:
        base_result['Present'] = False
        base_result['Note'] = "Absent (404 on all endpoints)"
    
    return base_result


# =============================================================================
# OUTPUT FUNCTIONS
# =============================================================================

def write_results_to_csv(results: List[Dict[str, Any]], output_path: Path):
    """
    Write the check results to a CSV file.
    
    Args:
        results: List of result dictionaries from check_row_status()
        output_path: Path where the CSV file should be written
    """
    if not results:
        logger.warning("No results to write")
        return

    # Define the columns in the output CSV
    fieldnames = [
        'InputRow',         # Row number from input file
        'TRN',              # Customer's Tax Registration Number
        'ApplicationNumber',# Registration number (e.g., SLB-156439)
        'YearChecked',      # Year that was checked (e.g., 2025)
        'EndpointUsed',     # Full API URL that was called
        'HTTPStatus',       # HTTP status code (200, 404, etc.)
        'Present',          # True if found, False if not
        'Note',             # Human-readable explanation
        'CheckedAt'         # Timestamp of the check
    ]
    
    # Write the CSV file
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()  # Write column headers
        writer.writerows(results)  # Write all result rows
    
    logger.info(f"Results written to {output_path.resolve()}")


# =============================================================================
# COMMAND LINE ARGUMENT PARSING
# =============================================================================

def parse_arguments():
    """
    Parse command-line arguments.
    
    Returns:
        Namespace object with the parsed arguments:
        - input_file: Path to the input file
        - output_dir: Directory for output CSV
        - dry_run: Boolean, if True skip API calls
        - debug: Boolean, if True enable debug logging
    """
    parser = argparse.ArgumentParser(
        description='Check application presence via SIDSP API',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m checker.run_checker input.xlsx
  python -m checker.run_checker data.csv --output-dir reports
  python -m checker.run_checker file.xlsx --dry-run
        """
    )
    
    # Required: Input file path
    parser.add_argument(
        'input_file',
        help='Path to input Excel (.xlsx, .xls) or CSV file'
    )
    
    # Optional: Output directory
    parser.add_argument(
        '--output-dir',
        default=OUTPUT_DIR,
        help=f'Directory for output CSV (default: {OUTPUT_DIR})'
    )
    
    # Optional: Dry run mode
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Load and validate input without making API calls'
    )
    
    # Optional: Debug mode
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    return parser.parse_args()


# =============================================================================
# MAIN EXECUTION FUNCTION
# =============================================================================

def run_checker():
    """
    Main execution logic for the application checker.
    
    This function:
    1. Parses command-line arguments
    2. Loads configuration
    3. Authenticates with the API
    4. Loads and processes the input file
    5. Writes results to CSV
    
    Handles errors gracefully and supports keyboard interrupt (Ctrl+C)
    to save partial results.
    """
    # -------------------------------------------------------------------------
    # STEP 1: Parse command-line arguments
    # -------------------------------------------------------------------------
    args = parse_arguments()
    
    # Enable debug logging if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Initialize variables
    results = []  # Will hold all check results
    client = None  # HTTP client (created later)
    
    try:
        # ---------------------------------------------------------------------
        # STEP 2: Load configuration from .env file
        # ---------------------------------------------------------------------
        settings = load_settings()
        logger.info(f"Base URL: {settings.base_url}")
        logger.info(f"Check Year: {settings.check_year}")
        
        # ---------------------------------------------------------------------
        # STEP 3: Initialize HTTP client and authenticate
        # ---------------------------------------------------------------------
        client = HttpClient(settings)
        
        if settings.token:
            # Use pre-existing token from environment
            client.set_static_token(settings.token)
            logger.info("Using static token from environment")
        else:
            # No token provided, need to login
            logger.info("No static token found, attempting login...")
            client.login_and_set_token()
            logger.info("Login successful")
        
        # ---------------------------------------------------------------------
        # STEP 4: Load input data from file
        # ---------------------------------------------------------------------
        logger.info(f"Loading data from {args.input_file}...")
        input_rows = load_input_data(args.input_file, settings.excel_header_row)
        logger.info(f"Loaded {len(input_rows)} rows")
        
        # ---------------------------------------------------------------------
        # STEP 5: Handle dry run mode
        # ---------------------------------------------------------------------
        if args.dry_run:
            logger.info("DRY RUN MODE - No API calls will be made")
            logger.info(f"Sample row: {input_rows[0] if input_rows else 'No data'}")
            logger.info("Dry run complete. Use without --dry-run to process.")
            return
        
        # ---------------------------------------------------------------------
        # STEP 6: Process each row
        # ---------------------------------------------------------------------
        logger.info("Starting API checks...")
        start_time = time.time()
        
        for i, row in enumerate(input_rows):
            # Show progress every 10 rows
            if i > 0 and i % 10 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed  # Rows per second
                remaining = len(input_rows) - i
                eta = remaining / rate if rate > 0 else 0  # Estimated time remaining
                logger.info(
                    f"Progress: {i}/{len(input_rows)} "
                    f"({i/len(input_rows)*100:.1f}%) "
                    f"| ETA: {eta/60:.1f}m"
                )
            
            # Check this row and add result to list
            result = check_row_status(client, row, settings.check_year)
            results.append(result)
        
        end_time = time.time()
        
        # ---------------------------------------------------------------------
        # STEP 7: Print summary statistics
        # ---------------------------------------------------------------------
        total_present = sum(1 for r in results if r.get('Present') is True)
        total_absent = sum(1 for r in results if r.get('Present') is False)
        total_error = sum(
            1 for r in results 
            if r.get('Present') is None or (
                r.get('HTTPStatus') not in [200, 404] and 
                r.get('Present') is False
            )
        )
        
        logger.info("-" * 50)
        logger.info(f"Processing complete in {end_time - start_time:.1f} seconds")
        logger.info(f"Total Rows: {len(results)}")
        logger.info(f"Present (200 OK): {total_present}")
        logger.info(f"Absent (404): {total_absent}")
        logger.info(f"Errors (Other): {total_error}")
        logger.info("-" * 50)
        
        # ---------------------------------------------------------------------
        # STEP 8: Write results to CSV
        # ---------------------------------------------------------------------
        output_path = Path(args.output_dir) / f"results_{datetime.now():%Y%m%d_%H%M%S}.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)  # Create output dir if needed
        write_results_to_csv(results, output_path)
        
    except KeyboardInterrupt:
        # User pressed Ctrl+C - save what we have so far
        logger.warning("Interrupted by user. Saving partial results...")
        if results:
            output_path = Path(args.output_dir) / f"results_partial_{datetime.now():%Y%m%d_%H%M%S}.csv"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            write_results_to_csv(results, output_path)
    
    except (RuntimeError, FileNotFoundError, ValueError) as e:
        # Configuration or input file errors
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    
    finally:
        # Always clean up the HTTP client
        if client:
            client.close()


# =============================================================================
# SCRIPT ENTRY POINT
# =============================================================================

if __name__ == '__main__':
    # This script must be run as a module to work with relative imports
    # Usage: python -m checker.run_checker <input_file>
    
    if __package__ is None:
        print(
            "ERROR: This script must be run as a module.\n"
            "Usage: python -m checker.run_checker <input_file>"
        )
        sys.exit(1)
    
    run_checker()
