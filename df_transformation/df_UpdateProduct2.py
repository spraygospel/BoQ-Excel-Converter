import pandas as pd
import os
import streamlit as st
import json
from config import OUTPUT_PATHS

# Path ke file konfigurasi
USER_CONFIG_FILE = "temp/user_config.json"

# Load Odoo config langsung dari JSON
def get_odoo_config():
    try:
        if os.path.exists(USER_CONFIG_FILE):
            with open(USER_CONFIG_FILE, 'r') as f:
                config = json.load(f)
                if 'odoo' in config:
                    print(f"[OK] Loaded Odoo config from {USER_CONFIG_FILE}")
                    return config['odoo']
                else:
                    print(f"[ERROR] No 'odoo' key found in {USER_CONFIG_FILE}")
    except Exception as e:
        print(f"[ERROR] Failed to load Odoo config: {str(e)}")
    
    # Default config jika gagal memuat dari file
    print("[INFO] Using default Odoo config")
    return {
        'url': 'https://api-odoo.visiniaga.com',
        'db': 'Final6.Copy250301',
        'username': 'od@visiniaga.com',
        'password': '',
        'enabled': True
    }


def process_second_table(df_filtered, odoo_data):
    """
    Process the second table for UpdateProduct - products with same vendor but different prices
    
    Args:
        df_filtered (pandas.DataFrame): Filtered DataFrame with products
        odoo_data (dict): Dictionary containing data from Odoo (products, supplier_info, etc.)
    
    Returns:
        pandas.DataFrame: DataFrame for the second table
    """
    print("\n=== PROCESSING SECOND TABLE: PRODUCTS WITH SAME VENDOR BUT DIFFERENT PRICES ===")
    
    # Load Odoo config di awal
    ODOO_CONFIG = get_odoo_config()
    print(f"Odoo Config in df_UpdateProduct2: URL={ODOO_CONFIG.get('url')}, Enabled={ODOO_CONFIG.get('enabled')}")

    # Create second table DataFrame with all required columns including the new SO number column
    df_update2 = pd.DataFrame(columns=[
        'id', 'default_code', 'name', 'seller_ids/id', 'seller_ids/price', 'seller_ids/x_studio_so_number'
    ])
    
    # Get SO Number from session state
    so_number = st.session_state.get('so_number', '')
    print(f"Using SO Number: {so_number}")
    
    if df_filtered is None or df_filtered.empty:
        print("No data for processing second table")
        # Store empty DataFrame in session state
        st.session_state.df_UpdateProduct_same_vendor = df_update2
        save_to_excel_second_table(df_update2)
        return df_update2
    
    # If Odoo is not connected or data is not available, return empty DataFrame
    if not odoo_data:
        print("No Odoo data available for processing second table")
        st.session_state.df_UpdateProduct_same_vendor = df_update2
        save_to_excel_second_table(df_update2)
        return df_update2
    
    # Get necessary columns
    internal_ref_col = find_column(df_filtered, "Internal References")
    description_col = find_column(df_filtered, "Description - Item yang Ditawarkan")
    supplier_col = find_column(df_filtered, "Supplier")
    modal_unit_col = find_column(df_filtered, "Modal Unit")
    
    if not all([internal_ref_col, modal_unit_col, supplier_col]):
        print("Missing required columns for second table processing")
        st.session_state.df_UpdateProduct_same_vendor = df_update2
        save_to_excel_second_table(df_update2)
        return df_update2
    
    # Process rows for the second table
    rows_to_add = []
    print("\n=== IDENTIFYING PRODUCTS WITH SAME VENDOR BUT DIFFERENT PRICES ===")
    
    # Check if we have the needed Odoo data
    if 'products' in odoo_data and 'supplier_info' in odoo_data:
        for idx, row in df_filtered.iterrows():
            default_code = row[internal_ref_col]
            supplier_name = row[supplier_col] if pd.notna(row[supplier_col]) else ""
            current_price = row[modal_unit_col] if pd.notna(row[modal_unit_col]) else ""
            product_name = row[description_col] if description_col and pd.notna(row[description_col]) else ""
            
            # Skip if empty values
            if not default_code or not supplier_name or not current_price:
                continue
                
            print(f"\nChecking product: {default_code}, Supplier: {supplier_name}, Price: {current_price}")
            
            # Skip if product not found in Odoo
            if default_code not in odoo_data['products']:
                print(f"Product {default_code} not found in Odoo, skipping")
                continue
                
            product = odoo_data['products'][default_code]
            product_id = product['id']
            template_id = product['product_tmpl_id'][0] if product.get('product_tmpl_id') else None
            
            if not template_id or template_id not in odoo_data['supplier_info']:
                print(f"No supplier info found for product {default_code}, template_id: {template_id}")
                continue
                
            # Use product name from the "Description - Item yang Ditawarkan" column
            # If empty, fallback to name from Odoo
            if not product_name:
                product_name = product.get('name', '')
            
            # Check for existing suppliers with price differences
            supplier_name_lower = supplier_name.lower()
            
            for supplier_id, info in odoo_data['supplier_info'][template_id].items():
                supplier_info_name = info.get('supplier_name', '').lower()
                
                # Check if this is the same supplier (using fuzzy matching)
                if (supplier_name_lower in supplier_info_name or 
                    supplier_info_name in supplier_name_lower or
                    supplier_name_lower == supplier_info_name):
                    
                    # Get prices for comparison
                    try:
                        current_price_float = float(current_price) if isinstance(current_price, (str, int)) else current_price
                        odoo_price = info['price']
                        
                        print(f"Found matching supplier: {info.get('supplier_name')}")
                        print(f"Price comparison: Current={current_price_float}, Odoo={odoo_price}")
                        
                        # Check if prices are different (with small tolerance)
                        if abs(odoo_price - current_price_float) > 0.01:
                            print(f"Price difference detected: Current={current_price_float}, Odoo={odoo_price}")
                            
                            # Get product XML ID (must be in format "export.product_product_XXX")
                            product_xml_id = ""
                            if product_id in odoo_data.get('xml_ids', {}):
                                product_xml_id = odoo_data['xml_ids'][product_id]
                            else:
                                product_xml_id = str(product_id)
                            
                            # Get supplierinfo XML ID (must be in format "export.product_supplierinfo_XXX")
                            if 'xml_id' in info and info['xml_id']:
                                supplier_info_id = info['xml_id']
                                # Pastikan memiliki format "export.product_supplierinfo_XXX"
                                if not supplier_info_id.startswith('export.'):
                                    supplier_info_id = 'export.' + supplier_info_id
                            else:
                                # Jika tidak ada xml_id, buat format yang sesuai dari ID numerik
                                supplier_info_id = f"export.product_supplierinfo_{info['id']}"
                            print(f"Using supplier_info_id: {supplier_info_id} for supplier {info.get('supplier_name')}")
                                                        
                            # Add to rows_to_add with SO Number
                            rows_to_add.append({
                                'id': product_xml_id,
                                'default_code': default_code,
                                'name': product_name,
                                'seller_ids/id': supplier_info_id,
                                'seller_ids/price': current_price_float,
                                'seller_ids/x_studio_so_number': so_number
                            })
                            print(f"Added to second table: {default_code} with supplier {info.get('supplier_name')}")
                            
                            # Only process the first matching supplier
                            break
                    except (ValueError, TypeError) as e:
                        print(f"Error comparing prices: {str(e)}")
    else:
        print("Required Odoo data is missing for second table processing")
    
    # Create DataFrame from rows
    if rows_to_add:
        df_update2 = pd.DataFrame(rows_to_add)
        print(f"\nCreated second table with {len(df_update2)} rows")
        
        # Print a sample of the data
        if len(df_update2) > 0:
            sample_size = min(5, len(df_update2))
            print(f"\nSample of second table (first {sample_size} rows):")
            for i in range(sample_size):
                row = df_update2.iloc[i]
                print(f"  {i+1}. Product: {row['default_code']}, Name: {row['name'][:30]}..., "
                      f"Supplier ID: {row['seller_ids/id']}, New Price: {row['seller_ids/price']}, "
                      f"SO Number: {row['seller_ids/x_studio_so_number']}")
    else:
        print("\nNo rows found for second table")
    
    # Ensure the DataFrame has the correct columns in the right order
    if df_update2.empty:
        df_update2 = pd.DataFrame(columns=[
            'id', 'default_code', 'name', 'seller_ids/id', 'seller_ids/price', 'seller_ids/x_studio_so_number'
        ])
    else:
        df_update2 = df_update2[[
            'id', 'default_code', 'name', 'seller_ids/id', 'seller_ids/price', 'seller_ids/x_studio_so_number'
        ]]
    
    # Clear NA values
    df_update2 = df_update2.fillna('')
    
    # Store in session state
    st.session_state.df_UpdateProduct_same_vendor = df_update2
    
    # Save to Excel
    save_to_excel_second_table(df_update2)
    
    # Print summary
    print("\nFinal df_update2 summary:")
    print(f"Shape: {df_update2.shape}")
    print("Non-empty values by column:")
    for col in df_update2.columns:
        non_empty = (df_update2[col] != '').sum()
        print(f"  - '{col}': {non_empty} non-empty values")
    
    return df_update2

def find_column(df, column_name):
    """
    Cari kolom dalam DataFrame dengan pencocokan case-insensitive
    
    Args:
        df (pd.DataFrame): DataFrame untuk dicari
        column_name (str): Nama kolom yang dicari
        
    Returns:
        str: Nama kolom asli jika ditemukan, None jika tidak
    """
    # Print columns we're searching through - helpful for debugging
    print(f"Searching for column '{column_name}' in columns: {list(df.columns)}")
    
    # 1. Exact match
    if column_name in df.columns:
        print(f"Found exact match for '{column_name}'")
        return column_name
    
    # 2. Case-insensitive exact match
    for col in df.columns:
        if col.lower() == column_name.lower():
            print(f"Found case-insensitive match: '{col}' for '{column_name}'")
            return col
    
    # 3. Substring match (untuk kolom dengan awalan/akhiran tambahan)
    for col in df.columns:
        if column_name.lower() in col.lower():
            print(f"Found substring match: '{col}' for '{column_name}'")
            return col
        # Also check if column is in the search term (reversed check)
        elif col.lower() in column_name.lower() and len(col) > 3:  # Minimum 4 chars to avoid short matches
            print(f"Found reverse substring match: '{col}' in '{column_name}'")
            return col
    
    # 4. Match dengan spasi fleksibel (misalnya "ModalUnit" untuk "Modal Unit")
    no_space_name = column_name.lower().replace(" ", "")
    for col in df.columns:
        no_space_col = col.lower().replace(" ", "")
        if no_space_name == no_space_col:
            print(f"Found no-space match: '{col}' for '{column_name}'")
            return col
        # Partial match without space (at least 70% of the characters)
        elif (no_space_name in no_space_col and len(no_space_name) >= 0.7 * len(no_space_col)) or \
             (no_space_col in no_space_name and len(no_space_col) >= 0.7 * len(no_space_name)):
            print(f"Found partial no-space match: '{col}' for '{column_name}'")
            return col
    
    # 5. Try fuzzy matching for similar column names with high similarity
    try:
        import difflib
        matches = difflib.get_close_matches(column_name.lower(), [c.lower() for c in df.columns], n=1, cutoff=0.7)
        if matches:
            for col in df.columns:
                if col.lower() == matches[0]:
                    print(f"Found fuzzy match: '{col}' for '{column_name}'")
                    return col
    except ImportError:
        pass  # Skip fuzzy matching if difflib is not available
    
    print(f"No match found for '{column_name}'")
    return None

def save_to_excel_second_table(df=None):
    """
    Save df_update2 to Excel file.
    
    Args:
        df (pandas.DataFrame, optional): DataFrame to save.
        
    Returns:
        str: Path to saved Excel file
    """
    if df is None or df.empty:
        print("No data to save for the second table")
        return None
    
    # Define path for the second table
    second_table_path = os.path.join(os.path.dirname(OUTPUT_PATHS['UpdateProduct']), "update_product_same_vendor.xlsx")
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(second_table_path), exist_ok=True)
    
    # Save to Excel
    try:
        df.to_excel(second_table_path, index=False)
        print(f"Second table DataFrame saved to {second_table_path}")
        return second_table_path
    except Exception as e:
        print(f"Error saving second table to Excel: {str(e)}")
        return None

if __name__ == "__main__":
    # For testing only
    print("This module is designed to be imported, not run directly.")
    print("To test, import and call process_second_table() with appropriate data.")
