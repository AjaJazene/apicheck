from typing import List, Tuple, Dict, Any

# Sequence format: (path, parameters_dict)
EndpointSequence = List[Tuple[str, Dict[str, str]]]

def select_endpoint_sequence(
    row: Dict[str, Any], 
    default_year: str = "2025"
) -> EndpointSequence:
    """
    Determine the sequence of API endpoints to check for a given row.
    
    Strategy:
    1. Try v2 endpoint with year + TRN (if year available)
    2. Try v1 endpoint with year + TRN (fallback)
    3. Try v2 endpoint with TRN only
    4. Try v1 endpoint with TRN only
    
    Args:
        row: Input data row with normalized column names
        default_year: Year to use if ACADEMIC_YEAR is not in row (default: "2025")
    
    Returns:
        List of (endpoint_path, parameters) tuples to check in order
    """
    customer_trn = str(row.get('CUSTOMER_TRN') or '').strip()
    
    if not customer_trn:
        return []
    
    sequence = []
    
    # Determine year: use row value if present and valid, otherwise use default
    year_raw = row.get('ACADEMIC_YEAR')
    year = None
    
    # Handle various year formats (string, int, float, NaN, None)
    if year_raw is not None:
        year_str = str(year_raw).strip()
        # Check if it's a valid year (not "nan", "NaN", empty, etc.)
        if year_str and year_str.lower() != 'nan' and year_str.isdigit():
            year = year_str
    
    # If no valid year from row, use default
    if not year:
        year = default_year
    
    # Strategy 1 & 2: Check with year + TRN (v2 then v1)
    if year:
        sequence.append((
            "/api/v2/Applications/ByYearTRN", 
            {"trn": customer_trn, "year": year}
        ))
        sequence.append((
            "/api/v1/Applications/ByYearTRN", 
            {"trn": customer_trn, "year": year}
        ))
    
    # Strategy 3 & 4: Check with TRN only (v2 then v1)
    sequence.append((
        "/api/v2/Applications/ByTRN", 
        {"trn": customer_trn}
    ))
    sequence.append((
        "/api/v1/Applications/ByTRN", 
        {"trn": customer_trn}
    ))
    
    return sequence