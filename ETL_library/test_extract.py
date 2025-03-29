import pandas as pd
import argparse
from extract import create_extractor

def main():
    parser = argparse.ArgumentParser(description="Test ETL extract components")
    parser.add_argument("file_path", help="Path to input file")
    parser.add_argument("--sheet-name", default=0, help="Sheet name or index (default: 0)")
    parser.add_argument("--header-row", type=int, help="Header row (1-based)")
    parser.add_argument("--data-start-row", type=int, help="Data start row (1-based)")
    parser.add_argument("--data-end-row", type=int, help="Data end row (1-based)")
    parser.add_argument("--header-start-col", type=int, help="Header start column (1-based)")
    parser.add_argument("--header-end-col", type=int, help="Header end column (1-based)")
    parser.add_argument("--disable-auto-detect", action="store_true", 
                        help="Disable automatic detection of data range using borders")
    parser.add_argument("--output", help="Path to save output Excel file (optional)")
    
    args = parser.parse_args()
    
    try:
        # Create appropriate extractor
        extractor = create_extractor(
            file_path=args.file_path,
            sheet_name=args.sheet_name,
            header_row=args.header_row,
            data_start_row=args.data_start_row,
            data_end_row=args.data_end_row,
            header_start_col=args.header_start_col,
            header_end_col=args.header_end_col,
            auto_detect_range=not args.disable_auto_detect
        )
        
        # Extract data
        result_df, border_info = extractor.extract()
        
        # Print information about detection
        print(f"File extracted successfully: {args.file_path}")
        
        if border_info:
            print("\nAuto-detection information:")
            print(f"  Header row: {border_info.get('header_row')}")
            print(f"  Data start row: {border_info.get('data_start_row')}")
            print(f"  Data end row: {border_info.get('data_end_row')}")
            
            if 'rows_with_bottom_border' in border_info:
                border_rows = sorted(list(border_info['rows_with_bottom_border'].keys()))
                if border_rows:
                    print(f"  Detected rows with bottom borders: {border_rows}")
        
        # Show data preview
        print("\nData preview:")
        print(f"Shape: {result_df.shape}")
        print(result_df.tail(7))
        
        # Save to output file if specified
        if args.output:
            result_df.to_excel(args.output, index=False)
            print(f"\nExtracted data saved to: {args.output}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
