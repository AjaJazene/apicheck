import sys
import logging
import time
import csv
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List
from urllib.parse import urlencode

from .config import load_settings
from .http_client import HttpClient
from .loader import load_input_data
from .selector import select_endpoint_sequence

# Configuration
LOG_LEVEL = logging.INFO
OUTPUT_DIR = "out"
RATE_LIMIT_MS = 250  # Sleep 250ms per call

logging.basicConfig(
    level=LOG_LEVEL, 
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


def check_row_status(
    client: HttpClient, 
    row: Dict[str, Any],
    default_year: str
) -> Dict[str, Any]:
    """
    Check if an application exists via API endpoints with fallback strategy.
    
    Returns a dictionary with the check result and metadata.
    """
    sequence = select_endpoint_sequence(row, default_year)
    
    base_result = {
        "InputRow": row.get('InputRow'),
        "TRN": row.get('CUSTOMER_TRN', ''),
        "ApplicationNumber": row.get('REGISTRATION_NO', ''),
        "YearInput": row.get('ACADEMIC_YEAR', ''),
        "EndpointUsed": "NO_STRATEGY",
        "HTTPStatus": 0,
        "Present": None,
        "Note": "No valid identifier (CUSTOMER_TRN) found",
        "CheckedAt": datetime.now().isoformat()
    }
    
    if not sequence:
        return base_result

    # Try each endpoint in sequence
    for path, params in sequence:
        if not params:
            continue
        
        # Rate limiting
        time.sleep(RATE_LIMIT_MS / 1000.0)
        
        # Make API call
        status, content_type, body = client.get_json(path, params=params)
        body_snippet = body[:200].strip()
        
        # Record which endpoint was used
        param_str = urlencode(params)
        base_result["EndpointUsed"] = f"{path}?{param_str}"
        base_result["HTTPStatus"] = status
        
        if status == 200:
            # Check if response actually contains data
            try:
                import json
                data = json.loads(body)
                
                # Debug logging
                logger.debug(f"Response data type: {type(data)}, value: {data}")
                
                # Check if data is empty in various ways
                is_empty = False
                
                if data is None:
                    is_empty = True
                elif isinstance(data, list) and len(data) == 0:
                    is_empty = True
                elif isinstance(data, dict) and len(data) == 0:
                    is_empty = True
                elif isinstance(data, dict):
                    # Check if dict has empty arrays/lists
                    for value in data.values():
                        if isinstance(value, list) and len(value) == 0:
                            is_empty = True
                            break
                
                if is_empty:
                    # Empty response means not found, continue to next endpoint
                    logger.debug(f"200 but empty response on {path}, trying next...")
                    continue
                
                # Valid data found
                base_result['Present'] = True
                base_result['Note'] = f"Present (200 OK with data)"
                logger.debug(f"Found data: {str(data)[:100]}")
                return base_result
                
            except (json.JSONDecodeError, ValueError) as e:
                # Can't parse JSON - treat as error
                base_result['Present'] = False
                base_result['Note'] = f"Error: Invalid JSON response - {str(e)[:50]}"
                return base_result
        
        elif status == 404:
            # Continue to next endpoint
            logger.debug(f"404 on {path}, trying next endpoint...")
            continue
        
        else:
            # Non-retryable error (401, 500, etc.)
            base_result['Present'] = False
            base_result['Note'] = f"Error ({status}): {body_snippet}"
            return base_result
    
    # All endpoints returned 404
    if base_result.get("Present") is None:
        base_result['Present'] = False
        base_result['Note'] = "Absent (404 on all endpoints)"
    
    return base_result


def write_results_to_csv(results: List[Dict[str, Any]], output_path: Path):
    """Write check results to CSV file."""
    if not results:
        logger.warning("No results to write")
        return

    fieldnames = [
        'InputRow', 'TRN', 'ApplicationNumber', 'YearInput', 
        'EndpointUsed', 'HTTPStatus', 'Present', 'Note', 'CheckedAt'
    ]
    
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    
    logger.info(f"Results written to {output_path.resolve()}")


def parse_arguments():
    """Parse command-line arguments."""
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
    
    parser.add_argument(
        'input_file',
        help='Path to input Excel (.xlsx, .xls) or CSV file'
    )
    
    parser.add_argument(
        '--output-dir',
        default=OUTPUT_DIR,
        help=f'Directory for output CSV (default: {OUTPUT_DIR})'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Load and validate input without making API calls'
    )
    
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Enable debug logging'
    )
    
    return parser.parse_args()


def run_checker():
    """Main execution logic."""
    args = parse_arguments()
    
    # Set log level
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    
    results = []
    client = None
    
    try:
        # Load configuration
        settings = load_settings()
        logger.info(f"Base URL: {settings.base_url}")
        logger.info(f"Check Year: {settings.check_year}")
        
        # Initialize HTTP client
        client = HttpClient(settings)
        
        if settings.token:
            client.set_static_token(settings.token)
            logger.info("Using static token from environment")
        else:
            logger.info("No static token found, attempting login...")
            client.login_and_set_token()
            logger.info("Login successful")
        
        # Load input data
        logger.info(f"Loading data from {args.input_file}...")
        input_rows = load_input_data(args.input_file, settings.excel_header_row)
        logger.info(f"Loaded {len(input_rows)} rows")
        
        # Dry run mode
        if args.dry_run:
            logger.info("DRY RUN MODE - No API calls will be made")
            logger.info(f"Sample row: {input_rows[0] if input_rows else 'No data'}")
            logger.info("Dry run complete. Use without --dry-run to process.")
            return
        
        # Process rows
        logger.info("Starting API checks...")
        start_time = time.time()
        
        for i, row in enumerate(input_rows):
            if i > 0 and i % 10 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed
                remaining = len(input_rows) - i
                eta = remaining / rate if rate > 0 else 0
                logger.info(
                    f"Progress: {i}/{len(input_rows)} "
                    f"({i/len(input_rows)*100:.1f}%) "
                    f"| ETA: {eta/60:.1f}m"
                )
            
            result = check_row_status(client, row, settings.check_year)
            results.append(result)
        
        end_time = time.time()
        
        # Summary statistics
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
        
        # Write output
        output_path = Path(args.output_dir) / f"results_{datetime.now():%Y%m%d_%H%M%S}.csv"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        write_results_to_csv(results, output_path)
        
    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Saving partial results...")
        if results:
            output_path = Path(args.output_dir) / f"results_partial_{datetime.now():%Y%m%d_%H%M%S}.csv"
            output_path.parent.mkdir(parents=True, exist_ok=True)
            write_results_to_csv(results, output_path)
    
    except (RuntimeError, FileNotFoundError, ValueError) as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
    
    finally:
        if client:
            client.close()


if __name__ == '__main__':
    if __package__ is None:
        print(
            "ERROR: This script must be run as a module.\n"
            "Usage: python -m checker.run_checker <input_file>"
        )
        sys.exit(1)
    
    run_checker()