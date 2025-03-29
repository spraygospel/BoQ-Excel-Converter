import pandas as pd
import numpy as np
from typing import Dict, List, Union, Optional, Any


class DataPreview:
    """
    Menghasilkan preview data untuk UI.
    """
    
    def __init__(
        self,
        max_rows: int = 10,
        include_stats: bool = True
    ):
        """
        Inisialisasi DataPreview.
        
        Args:
            max_rows: Jumlah baris untuk preview
            include_stats: Boolean untuk menyertakan statistik
        """
        self.max_rows = max_rows
        self.include_stats = include_stats
    
    def generate_preview(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Generate preview of DataFrame including sample rows and statistics.
        
        Args:
            df: DataFrame untuk dipreview
            
        Returns:
            Dictionary with preview data and statistics
        """
        if df.empty:
            return {
                'total_rows': 0,
                'total_columns': 0,
                'columns': [],
                'preview_data': [],
                'stats': {}
            }
        
        # Get basic info
        total_rows = len(df)
        total_columns = len(df.columns)
        columns = list(df.columns)
        
        # Generate preview data (sample rows)
        preview_rows = min(self.max_rows, total_rows)
        preview_data = df.head(preview_rows).replace({np.nan: None}).to_dict('records')
        
        result = {
            'total_rows': total_rows,
            'total_columns': total_columns,
            'columns': columns,
            'preview_data': preview_data
        }
        
        # Generate statistics if requested
        if self.include_stats:
            stats = {}
            
            # Generate basic statistics for numeric columns
            numeric_cols = df.select_dtypes(include=['number']).columns
            if len(numeric_cols) > 0:
                numeric_stats = df[numeric_cols].describe().to_dict()
                stats['numeric'] = numeric_stats
            
            # Generate statistics for categorical/text columns
            cat_cols = df.select_dtypes(exclude=['number']).columns
            cat_stats = {}
            
            for col in cat_cols:
                # Count unique values
                value_counts = df[col].value_counts().head(5).to_dict()
                # Count null values
                null_count = df[col].isna().sum()
                # Get sample of unique values
                unique_values = df[col].dropna().unique()
                unique_sample = unique_values[:5].tolist() if len(unique_values) > 0 else []
                
                cat_stats[col] = {
                    'unique_count': len(unique_values),
                    'null_count': null_count,
                    'null_percentage': (null_count / total_rows) * 100 if total_rows > 0 else 0,
                    'top_values': value_counts,
                    'unique_samples': unique_sample
                }
            
            stats['categorical'] = cat_stats
            
            # Add statistics to result
            result['stats'] = stats
        
        return result


class ConfigManager:
    """
    Mengelola konfigurasi untuk aplikasi ETL.
    """
    
    @staticmethod
    def load_mapping(mapping_file: str) -> Dict[str, str]:
        """
        Load field mapping from CSV file.
        
        Format CSV: source_field,target_field,default_value
        
        Args:
            mapping_file: Path to CSV mapping file
            
        Returns:
            Dictionary of {source_field: target_field}
        """
        mapping_df = pd.read_csv(mapping_file)
        
        # Basic validation
        required_cols = ['source_field', 'target_field']
        if not all(col in mapping_df.columns for col in required_cols):
            missing = [col for col in required_cols if col not in mapping_df.columns]
            raise ValueError(f"Mapping file missing required columns: {', '.join(missing)}")
        
        # Create mapping dictionary
        mapping = {}
        default_values = {}
        
        for _, row in mapping_df.iterrows():
            source = row['source_field']
            target = row['target_field']
            mapping[source] = target
            
            # Add default value if present
            if 'default_value' in mapping_df.columns and pd.notna(row.get('default_value')):
                default_values[target] = row['default_value']
        
        return mapping, default_values
