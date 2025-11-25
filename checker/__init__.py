"""
checker - SIDSP Application Presence Checker
=============================================

A Python package for checking if loan applications exist in the SIDSP system.

Modules:
--------
- config.py      : Configuration management (loads settings from .env)
- loader.py      : Input file loading (Excel/CSV with column normalization)
- http_client.py : HTTP client for API communication
- selector.py    : API endpoint selection logic
- run_checker.py : Main entry point and orchestration

Usage:
------
    python -m checker.run_checker input.xlsx
    python -m checker.run_checker data.csv --dry-run
    python -m checker.run_checker file.xlsx --debug

Workflow:
---------
1. Load configuration from .env file
2. Authenticate with SIDSP API (token or email/password)
3. Read input file (Excel or CSV)
4. For each row, check if application exists using TRN + Year
5. Write results to CSV file

Output:
-------
Results are written to the 'out/' directory as CSV files with columns:
- InputRow: Row number from input file
- TRN: Customer's Tax Registration Number
- ApplicationNumber: Registration number (e.g., SLB-156439)
- YearChecked: Academic year checked (e.g., 2025)
- Present: True if application found, False otherwise
- Note: Human-readable status message
"""

