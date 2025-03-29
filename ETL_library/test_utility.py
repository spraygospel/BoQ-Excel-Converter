import argparse
import pandas as pd
import logging
import json
import os
from typing import Dict, Any

# Import utility components
from utility import DataPreview, ConfigManager
from extract import create_extractor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('test_utility')


def test_data_preview(file_path: str, sheet_name: int = 0, max_rows: int = 10) -> Dict[str, Any]:
    """Test the DataPreview component."""
    logger.info("Testing DataPreview...")
    
    # Extract data
    extractor = create_extractor(
        file_path=file_path, 
        sheet_name=sheet_name,
        header_row=1,  # Explicit 1-based header row
        data_start_row=2  # Explicit 1-based data start row
    )
    if hasattr(extractor, 'extract') and callable(extractor.extract):
        result = extractor.extract()
        if isinstance(result, tuple):
            df = result[0]  # ExcelExtractor returns (df, border_info)
        else:
            df = result  # CSVExtractor returns just df
    else:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
    
    # Create DataPreview
    previewer = DataPreview(max_rows=max_rows, include_stats=True)
    preview_data = previewer.generate_preview(df)
    
    # Print preview info
    logger.info(f"DataFrame has {preview_data['total_rows']} rows and {preview_data['total_columns']} columns")
    logger.info(f"Columns: {', '.join(preview_data['columns'])}")
    logger.info(f"Showing preview of first {len(preview_data['preview_data'])} rows")
    
    # Save preview to JSON file
    output_file = os.path.splitext(os.path.basename(file_path))[0] + "_preview.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(preview_data, f, indent=2, default=str)
    logger.info(f"Preview data saved to {output_file}")
    
    return preview_data


def test_config_manager(mapping_file: str) -> Dict[str, str]:
    """Test the ConfigManager component."""
    logger.info("Testing ConfigManager...")
    
    # Load mapping
    try:
        mapping, default_values = ConfigManager.load_mapping(mapping_file)
        
        # Print mapping info
        logger.info(f"Loaded {len(mapping)} field mappings")
        for source, target in mapping.items():
            logger.info(f"  {source} -> {target}")
        
        # Print default values if any
        if default_values:
            logger.info(f"Loaded {len(default_values)} default values")
            for field, value in default_values.items():
                logger.info(f"  {field} = {value}")
        
        return mapping
    except Exception as e:
        logger.error(f"Error loading mapping: {str(e)}")
        raise


def main():
    parser = argparse.ArgumentParser(description='Test utility components')
    parser.add_argument('--file', required=True, help='Path to Excel file for preview')
    parser.add_argument('--sheet', type=int, default=0, help='Sheet index')
    parser.add_argument('--max-rows', type=int, default=10, help='Maximum rows for preview')
    parser.add_argument('--mapping-file', help='Path to mapping CSV file')
    
    args = parser.parse_args()
    
    # Test DataPreview
    preview_data = test_data_preview(
        file_path=args.file,
        sheet_name=args.sheet,
        max_rows=args.max_rows
    )
    
    # Test ConfigManager if mapping file is provided
    if args.mapping_file:
        mapping = test_config_manager(args.mapping_file)
    
    logger.info("Testing complete")


if __name__ == "__main__":
    main()
