"""
loader.py - Input File Loader
==============================
This module handles loading input data from Excel (.xlsx, .xls) or CSV files.
It normalizes column names so the rest of the application can work with 
consistent column names regardless of how they appear in the input file.

Supported Input Formats:
------------------------
- Excel files: .xlsx, .xls
- CSV files: .csv

Column Name Normalization:
--------------------------
Input files may have column names in various formats:
- "Customer TRN", "Customer_TRN", "customer_trn", "CUSTOMER_TRN"

This module normalizes all of these to a single canonical format:
- "CUSTOMER_TRN"

This way, the rest of the code can always reference columns by their 
canonical names without worrying about input file variations.
"""

import pandas as pd
from typing import List, Dict, Any
from pathlib import Path
import re


# =============================================================================
# COLUMN NAME NORMALIZATION
# =============================================================================

def normalize_header(header: str) -> str:
    """
    Standardize a column name by removing spaces/underscores and converting to uppercase.
    
    This makes column matching case-insensitive and format-insensitive.
    
    Examples:
        normalize_header("Customer TRN")   -> "CUSTOMERTRN"
        normalize_header("customer_trn")   -> "CUSTOMERTRN"
        normalize_header("Academic_year")  -> "ACADEMICYEAR"
        normalize_header("Registration No") -> "REGISTRATIONNO"
    
    Args:
        header: The original column name from the input file
        
    Returns:
        A normalized version of the column name (uppercase, no spaces/underscores)
    """
    # Remove all spaces and underscores using regex
    # [\s_]+ matches one or more whitespace characters or underscores
    normalized = re.sub(r'[\s_]+', '', header)
    
    # Remove any leading/trailing whitespace and convert to uppercase
    return normalized.strip().upper()


# =============================================================================
# MAIN DATA LOADER
# =============================================================================

def load_input_data(filepath: str, header_row: int = 0) -> List[Dict[str, Any]]:
    """
    Load input data from an Excel or CSV file and normalize column names.
    
    This function:
    1. Reads the file (Excel or CSV)
    2. Normalizes column names to canonical format
    3. Validates required columns are present
    4. Adds an InputRow column for tracking
    5. Returns data as a list of dictionaries
    
    Args:
        filepath: Path to the input file (.xlsx, .xls, or .csv)
        header_row: Which row contains column headers (0-indexed, default: 0)
        
    Returns:
        List of dictionaries, where each dict represents one row.
        Example: [
            {'InputRow': 1, 'CUSTOMER_TRN': '100379893', 'REGISTRATION_NO': 'SLB-156439', ...},
            {'InputRow': 2, 'CUSTOMER_TRN': '100671551', 'REGISTRATION_NO': 'SLB-157220', ...},
        ]
        
    Raises:
        FileNotFoundError: If the input file doesn't exist
        ValueError: If the file type is unsupported or required columns are missing
    """
    
    # -------------------------------------------------------------------------
    # STEP 1: Validate the file exists
    # -------------------------------------------------------------------------
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {filepath}")

    # -------------------------------------------------------------------------
    # STEP 2: Define the column name mapping
    # -------------------------------------------------------------------------
    # This maps normalized column names to the canonical names used by the app.
    # 
    # How it works:
    #   1. Input column "Customer_TRN" gets normalized to "CUSTOMERTRN"
    #   2. "CUSTOMERTRN" is looked up in this map
    #   3. It maps to "CUSTOMER_TRN" (the canonical name with underscore)
    #
    # Why do this? So we can use consistent column names in our code regardless
    # of how the input file formats its headers.
    
    COLUMN_MAP = {
        'CUSTOMERTRN': 'CUSTOMER_TRN',       # Customer's Tax Registration Number
        'REGISTRATIONNO': 'REGISTRATION_NO', # Application registration number (e.g., SLB-156439)
        'ACADEMICYEAR': 'ACADEMIC_YEAR',     # Academic year (e.g., 2025)
        'BENEFICIARYTRN': 'BENEFICIARY_TRN', # Beneficiary's TRN (if different from customer)
    }

    # -------------------------------------------------------------------------
    # STEP 3: Read the file based on its type
    # -------------------------------------------------------------------------
    # We specify dtype=str for identifier columns to prevent pandas from
    # converting them to numbers (which could cause issues with leading zeros
    # or scientific notation for large numbers like TRNs).
    
    if path.suffix in ['.xlsx', '.xls']:
        # Read Excel file
        df = pd.read_excel(
            filepath,
            header=header_row,  # Which row has column headers
            dtype={
                # Keep these columns as strings, not numbers
                'Customer_TRN': str,
                'BeneficiaryTRN': str,
                'Registration_No': str,
                'Academic_Year': str,
            }
        )
    elif path.suffix == '.csv':
        # Read CSV file
        df = pd.read_csv(
            filepath,
            dtype={
                'Customer_TRN': str,
                'BeneficiaryTRN': str,
                'Registration_No': str,
                'Academic_Year': str,
            }
        )
    else:
        raise ValueError(
            f"Unsupported file type: {path.suffix}. "
            "Only .csv, .xlsx, and .xls files are supported."
        )

    # -------------------------------------------------------------------------
    # STEP 4: Clean up the data
    # -------------------------------------------------------------------------
    # Remove rows that are completely empty (all NaN values)
    df.dropna(how='all', inplace=True)

    # -------------------------------------------------------------------------
    # STEP 5: Normalize column names
    # -------------------------------------------------------------------------
    # Build a mapping from original column names to normalized canonical names
    
    normalized_columns = {}
    
    for col in df.columns:
        # First, normalize the column name (remove spaces/underscores, uppercase)
        normalized_name = normalize_header(str(col))
        
        # Then, map it to the canonical name if one exists
        # If not in the map, keep the normalized name as-is
        final_name = COLUMN_MAP.get(normalized_name, normalized_name)

        # Only add this mapping if the destination name isn't already used
        # This prevents duplicate columns if input has both "Customer TRN" and "customer_trn"
        if final_name not in normalized_columns.values():
            normalized_columns[col] = final_name

    # Apply the column renaming
    df.rename(columns=normalized_columns, inplace=True)

    # -------------------------------------------------------------------------
    # STEP 6: Validate required columns are present
    # -------------------------------------------------------------------------
    # CUSTOMER_TRN is required because it's the main identifier we use to
    # look up applications in the API
    
    required_columns = ['CUSTOMER_TRN']
    missing = [col for col in required_columns if col not in df.columns]
    
    if missing:
        available = list(df.columns)
        raise ValueError(
            f"Required columns missing: {missing}. "
            f"Available columns after normalization: {available}"
        )

    # -------------------------------------------------------------------------
    # STEP 7: Add InputRow column for tracking
    # -------------------------------------------------------------------------
    # This helps us trace results back to the original input file
    # InputRow is 1-based (first data row = 1) to match Excel row numbers
    
    df.insert(0, 'InputRow', range(1, len(df) + 1))

    # -------------------------------------------------------------------------
    # STEP 8: Convert to list of dictionaries and return
    # -------------------------------------------------------------------------
    # Each dictionary represents one row from the input file
    # This format is convenient for iterating and accessing values by column name
    
    return df.to_dict('records')
