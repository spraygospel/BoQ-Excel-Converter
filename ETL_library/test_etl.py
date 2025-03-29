import os
import pandas as pd
import logging
import argparse
from typing import Dict, List, Optional, Tuple

# Import extraction, validation and loading components
from extract import create_extractor, ExcelExtractor, CSVExtractor
from validate import DataValidator, CrossFileValidator
from load import OdooConnector

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('etl_test.log')
    ]
)
logger = logging.getLogger('etl_test')


def load_excel(file_path: str, sheet_name: Optional[int] = 0, 
               header_row: Optional[int] = None, data_start_row: Optional[int] = None) -> pd.DataFrame:
    """Load an Excel file into a DataFrame using the ExtractComponent."""
    try:
        # Use create_extractor factory function to get appropriate extractor
        extractor = create_extractor(
            file_path=file_path,
            sheet_name=sheet_name,
            header_row=header_row,
            data_start_row=data_start_row,
            auto_detect_range=True
        )
        
        # Extract data
        if isinstance(extractor, ExcelExtractor):
            df, border_info = extractor.extract()
            # Log border detection info
            logger.info(f"Border detection info: header_row={border_info['header_row']}, " 
                      f"data_start_row={border_info['data_start_row']}, "
                      f"data_end_row={border_info['data_end_row']}")
        else:
            df = extractor.extract()
            
        logger.info(f"Loaded {file_path} with {len(df)} rows and {len(df.columns)} columns")
        return df
    except Exception as e:
        logger.error(f"Error loading {file_path}: {str(e)}")
        raise


def test_data_validator(df: pd.DataFrame) -> pd.DataFrame:
    """Test the DataValidator component."""
    logger.info("Testing DataValidator...")
    
    # Set up some validation rules
    validation_rules = {
        # Validate that a column isn't empty
        df.columns[0]: [
            {'type': 'not_null', 'message': f'Nilai {df.columns[0]} tidak boleh kosong'}
        ]
    }
    
    # If there's a numeric column, add a min_value rule
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        validation_rules[numeric_cols[0]] = [
            {'type': 'min_value', 'value': 0, 'message': f'Nilai {numeric_cols[0]} harus lebih dari 0'}
        ]
    
    # Create validator
    validator = DataValidator(validation_rules=validation_rules, error_handling="warn")
    
    # Run validation
    _, validation_report = validator.validate(df)
    
    # Log validation results
    if len(validation_report) > 0:
        logger.info(f"Found {len(validation_report)} validation issues")
        for _, issue in validation_report.iterrows():
            logger.warning(f"Validation issue: {issue['message']} at row {issue['row_index']}")
    else:
        logger.info("No validation issues found")
    
    # Save validation report to Excel
    validation_report.to_excel('validation_report.xlsx', index=False)
    logger.info("Validation report saved to validation_report.xlsx")
    
    return validation_report


def test_cross_file_validator(first_df: pd.DataFrame, second_df: pd.DataFrame, 
                            first_col: str, second_col: str) -> pd.DataFrame:
    """Test the CrossFileValidator component."""
    logger.info("Testing CrossFileValidator...")
    
    # Create validator
    validator = CrossFileValidator(error_handling="warn")
    
    # Run validation
    is_match, validation_report = validator.validate_matching_values(
        first_df=first_df,
        second_df=second_df,
        first_col=first_col,
        second_col=second_col,
        case_sensitive=False,
        label_first="File1",
        label_second="File2"
    )
    
    # Log validation results
    if is_match:
        logger.info(f"All values in {first_col} match with values in {second_col}")
    else:
        logger.warning(f"Found mismatches between {first_col} and {second_col}")
        # Log details of mismatches
        for _, issue in validation_report[validation_report['validation_type'] == 'unmatched_value'].iterrows():
            logger.warning(f"Mismatch: {issue['message']}")
    
    # Save validation report to Excel
    cross_validation_report_path = 'cross_validation_report.xlsx'
    validation_report.to_excel(cross_validation_report_path, index=False)
    logger.info(f"Cross-validation report saved to {cross_validation_report_path}")
    
    return validation_report


def test_odoo_connector(host: str, db: str, username: str, password: str) -> bool:
    """Test the OdooConnector component."""
    logger.info("Testing OdooConnector...")
    
    # Create connector
    connector = OdooConnector(
        host=host,
        db=db,
        username=username,
        password=password
    )
    
    # Try to connect
    success = connector.connect()
    
    if success:
        logger.info("Successfully connected to Odoo")
        
        # Test a simple search query
        try:
            partner_ids = connector.search('res.partner', [('customer_rank', '>', 0)], limit=5)
            logger.info(f"Found {len(partner_ids)} customers")
            
            if partner_ids:
                partners = connector.read('res.partner', partner_ids, ['name', 'email'])
                for partner in partners:
                    logger.info(f"Customer: {partner.get('name')} ({partner.get('email', 'No email')})")
        except Exception as e:
            logger.error(f"Error executing test query: {str(e)}")
            return False
    else:
        logger.error("Failed to connect to Odoo")
    
    return success


def main():
    parser = argparse.ArgumentParser(description='Test ETL components')
    parser.add_argument('--file1', required=True, help='Path to first Excel file')
    parser.add_argument('--file2', help='Path to second Excel file (for cross validation)')
    parser.add_argument('--first-col', help='Column from first file to validate')
    parser.add_argument('--second-col', help='Column from second file to validate')
    parser.add_argument('--sheet1', type=int, default=0, help='Sheet index for first file')
    parser.add_argument('--sheet2', type=int, default=0, help='Sheet index for second file')
    parser.add_argument('--header-row1', type=int, help='Header row for first file (1-based)')
    parser.add_argument('--data-start-row1', type=int, help='Data start row for first file (1-based)')
    parser.add_argument('--header-row2', type=int, help='Header row for second file (1-based)')
    parser.add_argument('--data-start-row2', type=int, help='Data start row for second file (1-based)')
    odoo_host = 'https://api-odoo.visiniaga.com'
    odoo_db = 'OdooDev'
    odoo_user = 'odoo2@visiniaga.com'
    odoo_password = 'PH8EQ?YF}<ac2A:T9n6%^*'
    
    args = parser.parse_args()
    
    # Load first file
    df1 = load_excel(
        args.file1, 
        args.sheet1, 
        header_row=args.header_row1, 
        data_start_row=args.data_start_row1
    )
    
    # Test DataValidator
    validation_report = test_data_validator(df1)
    
    # Test CrossFileValidator if second file is provided
    if args.file2 and args.first_col and args.second_col:
        df2 = load_excel(
            args.file2, 
            args.sheet2,
            header_row=args.header_row2, 
            data_start_row=args.data_start_row2
        )
        cross_validation_report = test_cross_file_validator(
            df1, df2, args.first_col, args.second_col
        )
    
    # Test OdooConnector if credentials are provided
    if all([odoo_host, odoo_db, odoo_user, odoo_password]):
        connection_status = test_odoo_connector(
            odoo_host, odoo_db, odoo_user, odoo_password
        )
        logger.info(f"Odoo connection status: {'Success' if connection_status else 'Failed'}")
    else:
        logger.info("Skipping Odoo connection test (missing credentials)")
    
    logger.info("Testing complete")


if __name__ == "__main__":
    main()
