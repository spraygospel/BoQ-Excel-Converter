import pandas as pd
import numpy as np
import argparse
import os
from extract import create_extractor
from transform import (
    WhitespaceCleaner,
    EmptyspaceCleaner,
    SectionExtractor,
    FieldMapper,
    DataFrameJoiner
)

def test_whitespace_cleaner(df, args):
    """Test WhitespaceCleaner component."""
    print("\n=== Testing WhitespaceCleaner ===")
    cleaner = WhitespaceCleaner(
        clean_rows=not args.disable_clean_rows,
        clean_cols=not args.disable_clean_cols,
        threshold=args.threshold
    )
    
    print(f"Original shape: {df.shape}")
    result_df = cleaner.clean(df)
    print(f"After cleaning shape: {result_df.shape}")
    
    if args.output_dir:
        output_path = os.path.join(args.output_dir, "whitespace_cleaned.xlsx")
        result_df.to_excel(output_path, index=False)
        print(f"Saved to: {output_path}")
    
    return result_df

def test_emptyspace_cleaner(df, args):
    """Test EmptyspaceCleaner component."""
    if not args.header_names:
        print("\n=== Skipping EmptyspaceCleaner (no header_names provided) ===")
        return df
    
    header_list = [h.strip() for h in args.header_names.split(',')]
    print(f"\n=== Testing EmptyspaceCleaner with headers: {header_list} ===")
    
    # Check if headers exist in DataFrame
    missing_headers = [h for h in header_list if h not in df.columns]
    if missing_headers:
        print(f"Warning: Headers not found in columns: {missing_headers}")
        print(f"Available columns: {df.columns.tolist()}")
        return df
    
    cleaner = EmptyspaceCleaner(header_names=header_list)
    
    print(f"Original shape: {df.shape}")
    result_df = cleaner.clean(df)
    print(f"After cleaning shape: {result_df.shape}")
    print(f"Rows removed: {df.shape[0] - result_df.shape[0]}")
    
    if args.output_dir:
        output_path = os.path.join(args.output_dir, "emptyspace_cleaned.xlsx")
        result_df.to_excel(output_path, index=False)
        print(f"Saved to: {output_path}")
    
    return result_df

def test_section_extractor(df, args):
    """Test SectionExtractor component."""
    if not args.section_indicator_col:
        print("\n=== Skipping SectionExtractor (no section_indicator_col provided) ===")
        return df
    
    print(f"\n=== Testing SectionExtractor with indicator col '{args.section_indicator_col}' ===")
    
    if args.section_indicator_col not in df.columns:
        print(f"Warning: Section indicator column '{args.section_indicator_col}' not found in columns: {df.columns.tolist()}")
        return df
    
    target_col = args.target_section_col or "Section"
    extractor = SectionExtractor(
        section_indicator_col=args.section_indicator_col,
        target_section_col=target_col,
        remove_section_rows=not args.keep_section_rows
    )
    
    print(f"Original shape: {df.shape}")
    result_df = extractor.extract(df)
    print(f"After extraction shape: {result_df.shape}")
    
    # Print unique sections
    if not result_df.empty and target_col in result_df.columns:
        sections = result_df[target_col].dropna().unique()
        print(f"Detected sections: {sections.tolist()}")
    
    if args.output_dir:
        output_path = os.path.join(args.output_dir, "section_extracted.xlsx")
        result_df.to_excel(output_path, index=False)
        print(f"Saved to: {output_path}")
    
    return result_df

def test_field_mapper(df, args):
    """Test FieldMapper component."""
    if not args.mapping_file:
        print("\n=== Skipping FieldMapper (no mapping_file provided) ===")
        return df
    
    print(f"\n=== Testing FieldMapper with mapping file '{args.mapping_file}' ===")
    
    try:
        # Load mapping from Excel or CSV
        if args.mapping_file.endswith('.xlsx') or args.mapping_file.endswith('.xls'):
            mapping_df = pd.read_excel(args.mapping_file)
        elif args.mapping_file.endswith('.csv'):
            mapping_df = pd.read_csv(args.mapping_file)
        else:
            print(f"Unsupported mapping file format: {args.mapping_file}")
            return df
        
        # Convert mapping DataFrame to dictionary
        mapping = {}
        default_values = {}
        
        # Expected columns: source_field, target_field, default_value
        for _, row in mapping_df.iterrows():
            if 'source_field' in mapping_df.columns and 'target_field' in mapping_df.columns:
                source = row['source_field']
                target = row['target_field']
                
                if pd.notna(source) and pd.notna(target):
                    mapping[source] = target
                
                # Add default value if provided
                if 'default_value' in mapping_df.columns and pd.notna(row['default_value']):
                    default_values[target] = row['default_value']
        
        if not mapping:
            print("No valid mappings found in mapping file")
            return df
        
        print(f"Loaded {len(mapping)} field mappings")
        if default_values:
            print(f"Loaded {len(default_values)} default values")
        
        mapper = FieldMapper(
            mapping=mapping,
            default_values=default_values
        )
        
        print(f"Original shape: {df.shape}")
        result_df = mapper.map_fields(df)
        print(f"After mapping shape: {result_df.shape}")
        print(f"New columns: {result_df.columns.tolist()}")
        
        if args.output_dir:
            output_path = os.path.join(args.output_dir, "field_mapped.xlsx")
            result_df.to_excel(output_path, index=False)
            print(f"Saved to: {output_path}")
        
        return result_df
    
    except Exception as e:
        print(f"Error in field mapping: {e}")
        return df

def test_dataframe_joiner(df1, df2, args):
    """Test DataFrameJoiner component."""
    if not df2 is not None or not args.left_key or not args.right_key:
        print("\n=== Skipping DataFrameJoiner (missing second dataframe or keys) ===")
        return df1
    
    print(f"\n=== Testing DataFrameJoiner ===")
    print(f"Left key: {args.left_key}, Right key: {args.right_key}, Join type: {args.join_type}")
    
    if args.left_key not in df1.columns:
        print(f"Warning: Left key '{args.left_key}' not found in first dataframe columns: {df1.columns.tolist()}")
        return df1
    
    if args.right_key not in df2.columns:
        print(f"Warning: Right key '{args.right_key}' not found in second dataframe columns: {df2.columns.tolist()}")
        return df1
    
    # Parse columns to add
    columns_to_add = None
    if args.columns_to_add:
        columns_to_add = [col.strip() for col in args.columns_to_add.split(',')]
    
    joiner = DataFrameJoiner(
        left_key=args.left_key,
        right_key=args.right_key,
        join_type=args.join_type,
        columns_to_add=columns_to_add,
        match_case=args.match_case
    )
    
    print(f"Left DataFrame shape: {df1.shape}")
    print(f"Right DataFrame shape: {df2.shape}")
    
    result_df = joiner.join(df1, df2)
    
    print(f"Joined DataFrame shape: {result_df.shape}")
    if columns_to_add:
        added_cols = [col for col in columns_to_add if col in result_df.columns]
        print(f"Added columns: {added_cols}")
    
    if args.output_dir:
        output_path = os.path.join(args.output_dir, "joined_data.xlsx")
        result_df.to_excel(output_path, index=False)
        print(f"Saved to: {output_path}")
    
    return result_df

def main():
    parser = argparse.ArgumentParser(description="Test ETL transform components")
    
    # Input files
    parser.add_argument("file_path", help="Path to input file")
    parser.add_argument("--second-file", help="Path to second input file (for join operations)")
    
    # Output directory
    parser.add_argument("--output-dir", help="Directory to save output files")
    
    # Extract parameters
    parser.add_argument("--sheet-name", default=0, help="Sheet name or index (default: 0)")
    parser.add_argument("--header-row", type=int, help="Header row (1-based)")
    parser.add_argument("--data-start-row", type=int, help="Data start row (1-based)")
    parser.add_argument("--data-end-row", type=int, help="Data end row (1-based)")
    
    # WhitespaceCleaner parameters
    parser.add_argument("--disable-clean-rows", action="store_true", help="Disable cleaning empty rows")
    parser.add_argument("--disable-clean-cols", action="store_true", help="Disable cleaning empty columns")
    parser.add_argument("--threshold", type=float, default=0.9, help="Threshold for empty cells (default: 0.9)")
    
    # EmptyspaceCleaner parameters
    parser.add_argument("--header-names", help="Comma-separated list of header names that must not be empty")
    
    # SectionExtractor parameters
    parser.add_argument("--section-indicator-col", help="Column that identifies section headers")
    parser.add_argument("--target-section-col", help="Name for the new section column")
    parser.add_argument("--keep-section-rows", action="store_true", help="Keep section header rows")
    
    # FieldMapper parameters
    parser.add_argument("--mapping-file", help="Path to field mapping file (Excel or CSV)")
    
    # DataFrameJoiner parameters
    parser.add_argument("--left-key", help="Key column in first DataFrame")
    parser.add_argument("--right-key", help="Key column in second DataFrame")
    parser.add_argument("--join-type", default="left", choices=["left", "right", "inner", "outer"], 
                        help="Join type (default: left)")
    parser.add_argument("--columns-to-add", help="Comma-separated list of columns to add from right DataFrame")
    parser.add_argument("--match-case", action="store_true", help="Enable case-sensitive matching")
    
    # Component selection
    parser.add_argument("--only", help="Only run specific component (whitespace, emptyspace, section, mapper, joiner)")
    
    args = parser.parse_args()
    
    # Create output directory if specified
    if args.output_dir and not os.path.exists(args.output_dir):
        os.makedirs(args.output_dir)
    
    try:
        # Extract data from first file
        print(f"Extracting data from: {args.file_path}")
        extractor1 = create_extractor(
            file_path=args.file_path,
            sheet_name=args.sheet_name,
            header_row=args.header_row,
            data_start_row=args.data_start_row,
            data_end_row=args.data_end_row
        )
        df1, _ = extractor1.extract()
        
        # Extract data from second file if specified
        df2 = None
        if args.second_file:
            print(f"Extracting data from second file: {args.second_file}")
            extractor2 = create_extractor(
                file_path=args.second_file,
                sheet_name=args.sheet_name,
                header_row=args.header_row,
                data_start_row=args.data_start_row,
                data_end_row=args.data_end_row
            )
            df2, _ = extractor2.extract()
        
        # Save original extracted data
        if args.output_dir:
            extracted_path = os.path.join(args.output_dir, "extracted.xlsx")
            df1.to_excel(extracted_path, index=False)
            print(f"Extracted data saved to: {extracted_path}")
        
        result_df = df1
        
        # Run components based on selection or all in sequence
        components = ["whitespace", "emptyspace", "section", "mapper", "joiner"]
        if args.only:
            components = [c for c in components if c == args.only]
        
        for component in components:
            if component == "whitespace":
                result_df = test_whitespace_cleaner(result_df, args)
            elif component == "emptyspace":
                result_df = test_emptyspace_cleaner(result_df, args)
            elif component == "section":
                result_df = test_section_extractor(result_df, args)
            elif component == "mapper":
                result_df = test_field_mapper(result_df, args)
            elif component == "joiner" and df2 is not None:
                result_df = test_dataframe_joiner(result_df, df2, args)
        
        # Save final result
        if args.output_dir:
            final_path = os.path.join(args.output_dir, "final_result.xlsx")
            result_df.to_excel(final_path, index=False)
            print(f"\nFinal result saved to: {final_path}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
