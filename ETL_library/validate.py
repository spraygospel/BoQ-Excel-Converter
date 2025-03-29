import pandas as pd
import numpy as np
from typing import Dict, List, Any, Union, Optional, Tuple, Callable


class DataValidator:
    """
    Memvalidasi data berdasarkan aturan bisnis.
    
    Attributes:
        validation_rules: Dictionary aturan validasi per kolom
        error_handling: Strategi penanganan error ("fail", "warn", "ignore")
    """
    
    def __init__(
        self,
        validation_rules: Dict[str, List[Dict[str, Any]]],
        error_handling: str = "warn"
    ):
        """
        Inisialisasi DataValidator.
        
        Args:
            validation_rules: Dictionary aturan validasi per kolom
                Format: {
                    'kolom1': [
                        {'type': 'not_null', 'message': 'Nilai tidak boleh kosong'},
                        {'type': 'min_value', 'value': 0, 'message': 'Nilai harus >= 0'}
                    ]
                }
            error_handling: Strategi penanganan error ("fail", "warn", "ignore")
        """
        self.validation_rules = validation_rules
        self.error_handling = error_handling
        
        # Validasi error_handling
        if error_handling not in ["fail", "warn", "ignore"]:
            raise ValueError("error_handling harus salah satu dari: 'fail', 'warn', 'ignore'")
    
    def validate(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Memvalidasi DataFrame berdasarkan aturan bisnis.
        
        Args:
            df: DataFrame untuk divalidasi
            
        Returns:
            Tuple dari (DataFrame hasil, DataFrame laporan validasi)
        """
        # Salin DataFrame
        result_df = df.copy()
        
        # Siapkan DataFrame untuk laporan validasi
        validation_results = []
        
        # Validasi untuk setiap kolom
        for col, rules in self.validation_rules.items():
            # Skip jika kolom tidak ada
            if col not in df.columns:
                if self.error_handling == "fail":
                    raise ValueError(f"Kolom '{col}' tidak ditemukan dalam DataFrame")
                elif self.error_handling == "warn":
                    validation_results.append({
                        'column': col,
                        'rule': 'column_exists',
                        'message': f"Kolom '{col}' tidak ditemukan dalam DataFrame",
                        'row_index': None,
                        'value': None,
                        'status': 'error'
                    })
                continue
            
            # Apply each rule to the column
            for rule in rules:
                rule_type = rule.get('type', '')
                
                # Validate based on rule type
                if rule_type == 'not_null':
                    # Check for null values
                    null_mask = df[col].isna() | (df[col].astype(str).str.strip() == '')
                    invalid_rows = df.index[null_mask].tolist()
                    
                    for row_idx in invalid_rows:
                        validation_results.append({
                            'column': col,
                            'rule': rule_type,
                            'message': rule.get('message', f"Nilai tidak boleh kosong di kolom '{col}'"),
                            'row_index': row_idx,
                            'value': None,
                            'status': 'error'
                        })
                
                elif rule_type == 'min_value':
                    min_val = rule.get('value', 0)
                    # Convert to numeric, coerce errors to NaN
                    numeric_col = pd.to_numeric(df[col], errors='coerce')
                    # Find rows with values less than min_val or NaN if can't be converted
                    invalid_mask = (numeric_col < min_val) | numeric_col.isna()
                    invalid_rows = df.index[invalid_mask].tolist()
                    
                    for row_idx in invalid_rows:
                        validation_results.append({
                            'column': col,
                            'rule': rule_type,
                            'message': rule.get('message', f"Nilai harus >= {min_val} di kolom '{col}'"),
                            'row_index': row_idx,
                            'value': df.at[row_idx, col],
                            'status': 'error'
                        })
                
                elif rule_type == 'max_value':
                    max_val = rule.get('value', 0)
                    # Convert to numeric, coerce errors to NaN
                    numeric_col = pd.to_numeric(df[col], errors='coerce')
                    # Find rows with values greater than max_val or NaN if can't be converted
                    invalid_mask = (numeric_col > max_val) | numeric_col.isna()
                    invalid_rows = df.index[invalid_mask].tolist()
                    
                    for row_idx in invalid_rows:
                        validation_results.append({
                            'column': col,
                            'rule': rule_type,
                            'message': rule.get('message', f"Nilai harus <= {max_val} di kolom '{col}'"),
                            'row_index': row_idx,
                            'value': df.at[row_idx, col],
                            'status': 'error'
                        })
                
                elif rule_type == 'regex':
                    pattern = rule.get('pattern', '')
                    if not pattern:
                        continue
                    
                    # Apply regex validation
                    invalid_mask = ~df[col].astype(str).str.match(pattern)
                    invalid_rows = df.index[invalid_mask].tolist()
                    
                    for row_idx in invalid_rows:
                        validation_results.append({
                            'column': col,
                            'rule': rule_type,
                            'message': rule.get('message', f"Nilai tidak sesuai pola di kolom '{col}'"),
                            'row_index': row_idx,
                            'value': df.at[row_idx, col],
                            'status': 'error'
                        })
                
                elif rule_type == 'in_list':
                    valid_values = rule.get('values', [])
                    if not valid_values:
                        continue
                    
                    # Check if values are in the list
                    invalid_mask = ~df[col].isin(valid_values)
                    invalid_rows = df.index[invalid_mask].tolist()
                    
                    for row_idx in invalid_rows:
                        validation_results.append({
                            'column': col,
                            'rule': rule_type,
                            'message': rule.get('message', f"Nilai harus salah satu dari {valid_values} di kolom '{col}'"),
                            'row_index': row_idx,
                            'value': df.at[row_idx, col],
                            'status': 'error'
                        })
                
                elif rule_type == 'custom':
                    # Custom validation function
                    custom_func = rule.get('function')
                    if not callable(custom_func):
                        continue
                    
                    # Apply custom function
                    try:
                        invalid_mask = ~df[col].apply(custom_func)
                        invalid_rows = df.index[invalid_mask].tolist()
                        
                        for row_idx in invalid_rows:
                            validation_results.append({
                                'column': col,
                                'rule': rule_type,
                                'message': rule.get('message', f"Validasi kustom gagal di kolom '{col}'"),
                                'row_index': row_idx,
                                'value': df.at[row_idx, col],
                                'status': 'error'
                            })
                    except Exception as e:
                        if self.error_handling == "fail":
                            raise ValueError(f"Error pada validasi kustom untuk kolom '{col}': {str(e)}")
                        elif self.error_handling == "warn":
                            validation_results.append({
                                'column': col,
                                'rule': rule_type,
                                'message': f"Error pada validasi kustom: {str(e)}",
                                'row_index': None,
                                'value': None,
                                'status': 'error'
                            })
        
        # Create validation report DataFrame
        validation_report = pd.DataFrame(validation_results)
        
        # Handle validation results based on error_handling strategy
        if validation_results and self.error_handling == "fail":
            error_count = len(validation_results)
            raise ValueError(f"Validasi gagal dengan {error_count} kesalahan. Lihat validation_report untuk detail.")
        
        return result_df, validation_report


class CrossFileValidator:
    """
    Memvalidasi data antar file berbeda.
    
    Attributes:
        validation_rules: List aturan validasi antar file
        file_identifiers: Identifier untuk file
        key_cols: Kolom kunci untuk validasi
    """
    
    def __init__(
        self,
        error_handling: str = "warn"
    ):
        """
        Inisialisasi CrossFileValidator.
        
        Args:
            error_handling: Strategi penanganan error ("fail", "warn", "ignore")
        """
        self.error_handling = error_handling
        
        # Validasi error_handling
        if error_handling not in ["fail", "warn", "ignore"]:
            raise ValueError("error_handling harus salah satu dari: 'fail', 'warn', 'ignore'")
    
    def validate_matching_values(
        self, 
        first_df: pd.DataFrame, 
        second_df: pd.DataFrame,
        first_col: str,
        second_col: str,
        case_sensitive: bool = False,
        label_first: str = "first",
        label_second: str = "second"
    ) -> Tuple[bool, pd.DataFrame]:
        """
        Memvalidasi apakah nilai di kolom dari dua DataFrame cocok.
        
        Args:
            first_df: DataFrame pertama
            second_df: DataFrame kedua
            first_col: Nama kolom di DataFrame pertama
            second_col: Nama kolom di DataFrame kedua
            case_sensitive: Boolean apakah case-sensitive 
            label_first: Label untuk DataFrame pertama
            label_second: Label untuk DataFrame kedua
            
        Returns:
            Tuple dari (status_valid, validation_report)
        """
        # Validasi keberadaan kolom
        if first_col not in first_df.columns:
            if self.error_handling == "fail":
                raise ValueError(f"Kolom '{first_col}' tidak ditemukan dalam DataFrame pertama")
            validation_report = pd.DataFrame([{
                'validation_type': 'column_exists',
                'file': label_first,
                'column': first_col,
                'status': 'error',
                'message': f"Kolom '{first_col}' tidak ditemukan dalam DataFrame pertama"
            }])
            return False, validation_report
        
        if second_col not in second_df.columns:
            if self.error_handling == "fail":
                raise ValueError(f"Kolom '{second_col}' tidak ditemukan dalam DataFrame kedua")
            validation_report = pd.DataFrame([{
                'validation_type': 'column_exists',
                'file': label_second,
                'column': second_col,
                'status': 'error',
                'message': f"Kolom '{second_col}' tidak ditemukan dalam DataFrame kedua"
            }])
            return False, validation_report
        
        # Get non-null values from both DataFrames
        first_values = first_df[first_col].dropna().astype(str)
        if not case_sensitive:
            first_values = first_values.str.lower()
        first_values = first_values[first_values.str.strip() != ''].unique().tolist()
        
        second_values = second_df[second_col].dropna().astype(str)
        if not case_sensitive:
            second_values = second_values.str.lower()
        second_values = second_values[second_values.str.strip() != ''].unique().tolist()
        
        # Find unmatched values
        first_unmatched = [val for val in first_values if val not in second_values]
        second_unmatched = [val for val in second_values if val not in first_values]
        
        # Prepare validation report
        validation_results = []
        
        # Check overall match status
        is_match = not first_unmatched and not second_unmatched
        validation_results.append({
            'validation_type': 'cross_file_match',
            'file': f"{label_first}/{label_second}",
            'column': f"{first_col}/{second_col}",
            'status': 'success' if is_match else 'error',
            'message': 'Data matches between files' if is_match else 'Data does not match between files'
        })
        
        # Report unmatched values from first file
        for val in first_unmatched:
            validation_results.append({
                'validation_type': 'unmatched_value',
                'file': label_first,
                'column': first_col,
                'value': val,
                'status': 'error',
                'message': f"Value '{val}' in {label_first}.{first_col} has no match in {label_second}.{second_col}"
            })
        
        # Report unmatched values from second file
        for val in second_unmatched:
            validation_results.append({
                'validation_type': 'unmatched_value',
                'file': label_second,
                'column': second_col,
                'value': val,
                'status': 'error',
                'message': f"Value '{val}' in {label_second}.{second_col} has no match in {label_first}.{first_col}"
            })
        
        # Create validation report DataFrame
        validation_report = pd.DataFrame(validation_results)
        
        # Handle validation results based on error_handling strategy
        if not is_match and self.error_handling == "fail":
            first_unmatched_count = len(first_unmatched)
            second_unmatched_count = len(second_unmatched)
            raise ValueError(
                f"Cross-file validation failed. "
                f"{first_unmatched_count} values from {label_first}.{first_col} and "
                f"{second_unmatched_count} values from {label_second}.{second_col} are unmatched. "
                f"See validation_report for details."
            )
        
        return is_match, validation_report
