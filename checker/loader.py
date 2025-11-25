import pandas as pd
from typing import List, Dict, Any
from pathlib import Path
import re

# Normalize header strings for consistent column matching across different input files.
# This removes spaces/underscores and converts to uppercase, making downstream checks
# insensitive to variations like "Customer TRN", "customer_trn", or "CUSTOMERTRN".
def normalize_header(header: str) -> str:
    """Standardize column names by removing spaces/underscores and converting to uppercase."""
    return re.sub(r'[\s_]+', '', header).strip().upper()

def load_input_data(filepath: str, header_row: int = 0) -> List[Dict[str, Any]]:
    """
    Load input data from Excel or CSV file and normalize column names.

    Steps performed:
    1. Validate the input file exists.
    2. Define a canonical column map (COLUMN_MAP) to translate common header variants
       to the canonical internal names the rest of the application expects.
    3. Read the file (.xlsx/.xls via read_excel or .csv via read_csv) with safe dtypes
       to avoid numeric coercion for identifiers (TRNs, registration numbers).
    4. Drop rows that are completely empty.
    5. Normalize the dataframe column names using normalize_header() and COLUMN_MAP.
       Avoid creating duplicate columns when different inputs map to the same canonical name.
    6. Validate required columns are present after normalization; raise a clear error if not.
    7. Insert an InputRow column (1-based) to help track original row positions for reporting.
    8. Return records as a list of dictionaries (one dict per input row).

    Args:
        filepath: Path to the input spreadsheet or CSV.
        header_row: Zero-indexed row number used as the header when reading Excel.

    Returns:
        List of dicts representing rows with standardized column names.

    Raises:
        FileNotFoundError: If the provided file does not exist.
        ValueError: If the file type is unsupported or required columns are missing.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {filepath}")

    # COLUMN_MAP:
    # Map normalized incoming column names to the canonical names used by the app.
    # Example: an incoming header "Customer TRN" -> normalize_header -> "CUSTOMERTRN"
    # which then maps to "CUSTOMER_TRN".
    COLUMN_MAP = {
        'CUSTOMERTRN': 'CUSTOMER_TRN',
        'REGISTRATIONNO': 'REGISTRATION_NO',
        'ACADEMICYEAR': 'ACADEMIC_YEAR',
        'BENEFICIARYTRN': 'BENEFICIARY_TRN',
    }

    # Read the file depending on its extension. Use dtype to keep identifier-like columns as strings.
    if path.suffix in ['.xlsx', '.xls']:
        df = pd.read_excel(
            filepath,
            header=header_row,
            dtype={
                # dtype keys use expected column names as they might appear in the file.
                # Keeping them as strings prevents pandas from converting large numeric TRNs.
                'Customer_TRN': str,
                'BeneficiaryTRN': str,
                'Registration_No': str,
                'Academic_Year': str,
            }
        )
    elif path.suffix == '.csv':
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

    # Remove rows that have no data at all.
    df.dropna(how='all', inplace=True)

    # Build mapping from original dataframe column names to normalized canonical names.
    normalized_columns = {}
    for col in df.columns:
        normalized_name = normalize_header(str(col))
        final_name = COLUMN_MAP.get(normalized_name, normalized_name)

        # Only add mapping if it won't create duplicate destination column names.
        # This prevents collisions when multiple source columns map to the same canonical name.
        if final_name not in normalized_columns.values():
            normalized_columns[col] = final_name

    # Rename the dataframe columns in-place using the mapping we built.
    df.rename(columns=normalized_columns, inplace=True)

    # Ensure required columns are present after normalization.
    required_columns = ['CUSTOMER_TRN']
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        available = list(df.columns)
        raise ValueError(
            f"Required columns missing: {missing}. "
            f"Available columns after normalization: {available}"
        )

    # Insert a 1-based InputRow index at the front to help trace results back to the input file.
    df.insert(0, 'InputRow', range(1, len(df) + 1))

    # Return a list of dictionaries (one per row) which is convenient for downstream processing.
    return df.to_dict('records')
