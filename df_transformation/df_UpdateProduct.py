import pandas as pd
import pickle
import json
import os
import xmlrpc.client
import streamlit as st
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

# Import the second table processor
from df_transformation.df_UpdateProduct2 import process_second_table

# Initialize global variables
products_missing_xml_id = []
products_with_empty_id = []

def transform():
    """
    Transform df_base data into UpdateProduct format.
    Automatically connects to Odoo if enabled in config.
    
    Returns:
        pandas.DataFrame: DataFrame with UpdateProduct format (not a tuple)
    """
    # Load Odoo config at transform time (not module load time)
    ODOO_CONFIG = get_odoo_config()
    print(f"[TRANSFORM] Using Odoo Config: URL={ODOO_CONFIG.get('url')}, DB={ODOO_CONFIG.get('db')}, User={ODOO_CONFIG.get('username')}, Enabled={ODOO_CONFIG.get('enabled')}")
    
    # Declare globals at the beginning of function
    global products_missing_xml_id, products_with_empty_id
    
    # Reset the global variables
    products_missing_xml_id = []
    products_with_empty_id = []
    
    # Load df_base.pkl
    df_base_path = os.path.join(os.path.dirname(OUTPUT_PATHS['UpdateProduct']), "df_base.pkl")
    
    try:
        print(f"Loading df_base from {df_base_path}")
        with open(df_base_path, 'rb') as f:
            data = pickle.load(f)
            
        # Check if data is a dictionary or DataFrame
        if isinstance(data, dict):
            print("df_base is stored as a dictionary with keys:", list(data.keys()))
            # If it's a dictionary, we need to get the boq_df
            if 'boq_df' in data:
                df_base = data['boq_df']
                print(f"Using 'boq_df' from dictionary, with {len(df_base)} rows")
            else:
                # Try the first value if boq_df doesn't exist
                df_base = next(iter(data.values()))
                print(f"Using first DataFrame from dictionary, with {len(df_base)} rows")
        else:
            # It's directly a DataFrame
            df_base = data
            print(f"Loaded DataFrame directly, with {len(df_base)} rows")
        
    except Exception as e:
        print(f"Error loading df_base: {str(e)}")
        return None
    
    if df_base is None or df_base.empty:
        print("No data in df_base or file not found")
        return None
    
    # Debug: Print all columns exactly as they appear
    print("=" * 50)
    print("COLUMNS IN df_base (exact names):")
    for col in df_base.columns:
        print(f"  - '{col}'")
    print("=" * 50)
    
    # 1. Filter rows where "Internal References" is not empty
    internal_ref_col = find_column(df_base, "Internal References")
    
    if internal_ref_col:
        # Filter out rows where "Internal References" is empty, NULL, or "0"
        df_filtered = df_base[
            df_base[internal_ref_col].notna() & 
            (df_base[internal_ref_col] != "") & 
            (df_base[internal_ref_col] != "0")
        ].copy()
        
        if df_filtered.empty:
            print("No rows with valid 'Internal References' found")
            df_update1 = pd.DataFrame(columns=['id', 'default_code', 'name', 'seller_ids', 'seller_ids/price', 'seller_ids/x_studio_so_number'])
            # Store the second table in session state with empty DataFrame
            if 'df_UpdateProduct_same_vendor' not in st.session_state:
                st.session_state.df_UpdateProduct_same_vendor = pd.DataFrame(
                    columns=['id', 'default_code', 'name', 'seller_ids/id', 'seller_ids/price']
                )
            return df_update1
    else:
        print("Column 'Internal References' not found in df_base")
        df_update1 = pd.DataFrame(columns=['id', 'default_code', 'name', 'seller_ids', 'seller_ids/price', 'seller_ids/x_studio_so_number'])
        # Store the second table in session state with empty DataFrame
        if 'df_UpdateProduct_same_vendor' not in st.session_state:
            st.session_state.df_UpdateProduct_same_vendor = pd.DataFrame(
                columns=['id', 'default_code', 'name', 'seller_ids/id', 'seller_ids/price']
            )
        return df_update1
    
    # Create df_update1 with specified columns in the correct order
    df_update1 = pd.DataFrame(columns=['id', 'default_code', 'name', 'seller_ids', 'seller_ids/price', 'seller_ids/x_studio_so_number'])
    
    # 2. Map columns from df_filtered to df_update1
    
    # Map "Internal References" to "default_code"
    df_update1['default_code'] = df_filtered[internal_ref_col]
    
    # Map "Description - Item yang Ditawarkan" to "name"
    description_col = find_column(df_filtered, "Description - Item yang Ditawarkan")
    if description_col:
        df_update1['name'] = df_filtered[description_col]
    else:
        print("Column 'Description - Item yang Ditawarkan' not found")
        df_update1['name'] = ""
    
    # Map "Supplier" to "seller_ids"
    supplier_col = find_column(df_filtered, "Supplier")
    if supplier_col:
        df_update1['seller_ids'] = df_filtered[supplier_col]
    else:
        print("Column 'Supplier' not found")
        df_update1['seller_ids'] = ""
    
    # Map "Modal Unit" to "seller_ids/price" with enhanced debugging
    print("\nDEBUGGING MODAL UNIT COLUMN:")
    print("-" * 40)
    
    # Try exact match first
    modal_unit_col = find_column(df_filtered, "Modal Unit")
    if modal_unit_col:
        print(f"Found 'Modal Unit' column: {modal_unit_col}")
        print(f"Sample values: {df_filtered[modal_unit_col].head(3).tolist()}")
        
        # Direct assignment
        df_update1['seller_ids/price'] = df_filtered[modal_unit_col]
    else:
        print("Column 'Modal Unit' not found")
        df_update1['seller_ids/price'] = ""
    
    # Initialize empty id column
    df_update1['id'] = ""
    
    # Connect to Odoo and get data if enabled
    odoo_data = {}
    
    if ODOO_CONFIG.get('enabled', False):
        try:
            print(f"Connecting to Odoo at {ODOO_CONFIG.get('url', '')}...")
            
            # Connect to Odoo
            common = xmlrpc.client.ServerProxy(f"{ODOO_CONFIG.get('url', '')}/xmlrpc/2/common")
            uid = common.authenticate(
                ODOO_CONFIG.get('db', ''), 
                ODOO_CONFIG.get('username', ''), 
                ODOO_CONFIG.get('password', ''), 
                {}
            )
            
            if not uid:
                print("Odoo authentication failed. Please check credentials.")
            else:
                print(f"Successfully connected to Odoo as {ODOO_CONFIG['username']} (uid: {uid})")
                
                odoo_url = ODOO_CONFIG.get('url', '').rstrip('/')
                models = xmlrpc.client.ServerProxy(f"{odoo_url}/xmlrpc/2/object")
                print(f"Connecting to Odoo models at: {odoo_url}/xmlrpc/2/object")
                
                # Get all default_codes we need to search for
                default_codes = df_update1['default_code'].dropna().unique().tolist()
                
                # Get all supplier names we need to search for
                supplier_names = df_update1['seller_ids'].dropna().unique().tolist()
                
                # Create a dictionary to store all collected data
                odoo_data = {
                    'products': {},
                    'suppliers': {},
                    'xml_ids': {},
                    'supplier_info': {}
                }
                
                if default_codes:
                    print(f"Batch searching for {len(default_codes)} products by default_code...")
                    
                    # 1. BATCH GET PRODUCTS - Find all products by default_code in one call
                    product_records = models.execute_kw(
                        ODOO_CONFIG.get('db', ''), 
                        uid, 
                        ODOO_CONFIG.get('password', ''),
                        'product.product',
                        'search_read',
                        [[['default_code', 'in', default_codes]]],
                        {'fields': ['id', 'default_code', 'product_tmpl_id', 'name']}
                    )
                    
                    print(f"Found {len(product_records)} products in Odoo")
                    
                    # Store products by default_code
                    for record in product_records:
                        if 'default_code' in record and record['default_code']:
                            odoo_data['products'][record['default_code']] = record
                    
                    # Get res_ids for XML ID lookup
                    res_ids = [record['id'] for record in product_records]
                    template_ids = [record['product_tmpl_id'][0] for record in product_records if 'product_tmpl_id' in record]
                    
                    # 2. BATCH GET XML IDs - Get all XML IDs for products in one call
                    if res_ids:
                        print(f"Batch searching for XML IDs for {len(res_ids)} products...")
                        xml_ids = models.execute_kw(
                            ODOO_CONFIG['db'],
                            uid,
                            ODOO_CONFIG['password'],
                            'ir.model.data',
                            'search_read',
                            [[
                                ['model', '=', 'product.product'],
                                ['res_id', 'in', res_ids]
                            ]],
                            {'fields': ['name', 'module', 'res_id']}
                        )
                        
                        print(f"Found {len(xml_ids)} XML IDs for products")
                        
                        # Store XML IDs by res_id
                        for item in xml_ids:
                            res_id = item['res_id']
                            xml_id = f"{item['module']}.{item['name']}"
                            odoo_data['xml_ids'][res_id] = xml_id
                    
                    # 3. BATCH GET SUPPLIERS - Find all suppliers in one call
                    if supplier_names:
                        print(f"Batch searching for {len(supplier_names)} suppliers...")
                        supplier_records = models.execute_kw(
                            ODOO_CONFIG['db'],
                            uid,
                            ODOO_CONFIG['password'],
                            'res.partner',
                            'search_read',
                            [[['name', 'in', supplier_names], ['supplier_rank', '>', 0]]],
                            {'fields': ['id', 'name']}
                        )
                        
                        print(f"Found {len(supplier_records)} suppliers in Odoo")
                        
                        # Store suppliers by name (case-insensitive)
                        for record in supplier_records:
                            if 'name' in record and record['name']:
                                odoo_data['suppliers'][record['name'].lower()] = record
                    
                    # 4. BATCH GET SUPPLIER INFO - For all template_ids in one call
                    if template_ids:
                        print(f"Batch searching for supplier info for {len(template_ids)} product templates...")
                        supplier_info_records = models.execute_kw(
                            ODOO_CONFIG['db'],
                            uid,
                            ODOO_CONFIG['password'],
                            'product.supplierinfo',
                            'search_read',
                            [[['product_tmpl_id', 'in', template_ids]]],
                            {'fields': ['id', 'name', 'product_tmpl_id', 'price', 'product_code', 'product_name']}
                        )
                        
                        print(f"Found {len(supplier_info_records)} supplier info records")
                        
                        # Get XML IDs for supplier info
                        if supplier_info_records:
                            supplierinfo_ids = [record['id'] for record in supplier_info_records]
                            supplierinfo_xml_ids = models.execute_kw(
                                ODOO_CONFIG['db'],
                                uid,
                                ODOO_CONFIG['password'],
                                'ir.model.data',
                                'search_read',
                                [[
                                    ['model', '=', 'product.supplierinfo'],
                                    ['res_id', 'in', supplierinfo_ids]
                                ]],
                                {'fields': ['name', 'module', 'res_id']}
                            )
                            
                            # Store XML IDs for supplier info
                            supplierinfo_xml_id_map = {}
                            for item in supplierinfo_xml_ids:
                                res_id = item['res_id']
                                xml_id = f"{item['module']}.{item['name']}"
                                supplierinfo_xml_id_map[res_id] = xml_id
                        
                        # Get supplier names for all supplier info records
                        supplier_partner_ids = list(set([record['name'][0] for record in supplier_info_records if 'name' in record]))
                        supplier_partners = models.execute_kw(
                            ODOO_CONFIG['db'],
                            uid,
                            ODOO_CONFIG['password'],
                            'res.partner',
                            'read',
                            [supplier_partner_ids],
                            {'fields': ['id', 'name']}
                        )
                        
                        supplier_name_map = {partner['id']: partner['name'] for partner in supplier_partners}
                        
                        # Organize supplier info by template_id and supplier_id
                        for record in supplier_info_records:
                            tmpl_id = record['product_tmpl_id'][0]
                            supplier_id = record['name'][0]
                            supplier_name = supplier_name_map.get(supplier_id, "Unknown")
                            
                            # Add XML ID if available
                            if record['id'] in supplierinfo_xml_id_map:
                                record['xml_id'] = supplierinfo_xml_id_map[record['id']]
                            else:
                                record['xml_id'] = str(record['id'])
                            
                            record['supplier_name'] = supplier_name
                            
                            if tmpl_id not in odoo_data['supplier_info']:
                                odoo_data['supplier_info'][tmpl_id] = {}
                            
                            odoo_data['supplier_info'][tmpl_id][supplier_id] = record
                    
                    # Proses XML IDs untuk semua produk terlebih dahulu
                    for idx, row in df_update1.iterrows():
                        default_code = row['default_code']
                        if default_code in odoo_data['products']:
                            product = odoo_data['products'][default_code]
                            product_id = product['id']
                            
                            # Set XML ID or database ID (as fallback)
                            if product_id in odoo_data['xml_ids']:
                                df_update1.at[idx, 'id'] = odoo_data['xml_ids'][product_id]
                            else:
                                df_update1.at[idx, 'id'] = str(product_id)
                                # Track products without XML IDs
                                products_missing_xml_id.append({
                                    'default_code': default_code,
                                    'name': product.get('name', 'Unknown'),
                                    'id': product_id,
                                    'status': 'Tanpa XML ID'
                                })
                        else:
                            # Product not found in Odoo
                            products_with_empty_id.append({
                                'default_code': default_code,
                                'name': row['name'],
                                'id': 'KOSONG',
                                'status': 'ID Tidak Ditemukan'
                            })
                    
                    # Ubah logika pemrosesan baris untuk seleksi yang benar
                    filtered_rows = []
                    print("\n=== PROCESSING PRODUCTS FOR NEW VENDORS ===")
                    for idx, row in df_update1.iterrows():
                        default_code = row['default_code']
                        supplier_name = row['seller_ids']
                        current_price = row['seller_ids/price']
                        
                        print(f"\nProcessing product: {default_code}, Supplier: {supplier_name}, Price: {current_price}")
                        
                        # Skip jika supplier kosong
                        if pd.isna(supplier_name) or supplier_name == "":
                            print(f"Skipping row {idx}: Empty supplier name")
                            continue
                        
                        # Jika produk tidak ditemukan di Odoo, tambahkan ke filtered_rows
                        # (sehingga masih muncul di output, meski mungkin tidak bisa diupdate)
                        if default_code not in odoo_data['products']:
                            print(f"Product {default_code} not found in Odoo, but keeping for reference")
                            filtered_rows.append(idx)
                            continue
                            
                        product = odoo_data['products'][default_code]
                        product_id = product['id']
                        template_id = product['product_tmpl_id'][0] if product.get('product_tmpl_id') else None
                        
                        print(f"Found product in Odoo: ID={product_id}, Template ID={template_id}")
                        
                        # Periksa apakah supplier sudah terdaftar untuk produk ini
                        supplier_exists = False
                        
                        if template_id and template_id in odoo_data['supplier_info']:
                            # Dapatkan semua vendor yang sudah terdaftar untuk produk ini
                            existing_suppliers = []
                            for info_supplier_id, info in odoo_data['supplier_info'][template_id].items():
                                if 'supplier_name' in info:
                                    existing_suppliers.append(info['supplier_name'])
                            
                            print(f"Existing suppliers for this product: {existing_suppliers}")
                            
                            # Cari supplier ID yang cocok dengan nama supplier
                            supplier_id = None
                            supplier_name_lower = supplier_name.lower()
                            supplier_match = None
                            
                            # Cek jika supplier sudah ada dalam daftar existing_suppliers
                            for existing_supplier in existing_suppliers:
                                if (supplier_name_lower in existing_supplier.lower() or 
                                    existing_supplier.lower() in supplier_name_lower or
                                    supplier_name_lower == existing_supplier.lower()):
                                    supplier_exists = True
                                    supplier_match = existing_supplier
                                    print(f"Supplier '{supplier_name}' already exists as '{supplier_match}'")
                                    break
                            
                            if not supplier_exists:
                                # Coba cari supplier di semua supplier
                                for s_name, s_data in odoo_data['suppliers'].items():
                                    if (supplier_name_lower in s_name.lower() or 
                                        s_name.lower() in supplier_name_lower or
                                        supplier_name_lower == s_name.lower()):
                                        supplier_id = s_data['id']
                                        print(f"Supplier '{supplier_name}' exists in Odoo as '{s_name}' but not for this product")
                                        break
                            
                            # Jika supplier ditemukan, periksa apakah sudah terdaftar untuk produk ini
                            if supplier_id:
                                for info_supplier_id, info in odoo_data['supplier_info'][template_id].items():
                                    if info_supplier_id == supplier_id:
                                        supplier_exists = True
                                        print(f"Supplier already registered for this product")
                                        break
                        else:
                            print(f"No supplier info found for template_id {template_id}")
                        
                        # Jika supplier belum terdaftar untuk produk ini, masukkan ke filtered_rows
                        if not supplier_exists:
                            filtered_rows.append(idx)
                            print(f"Adding product {default_code} with supplier {supplier_name} to update list (supplier not registered for this product)")
                    
                    # Buat df_update1 baru hanya dengan baris yang perlu diupdate
                    if filtered_rows:
                        original_count = len(df_update1)
                        df_update1 = df_update1.loc[filtered_rows].reset_index(drop=True)
                        print(f"\nSelected {len(filtered_rows)} rows out of {original_count} for update where supplier is not registered")
                        
                        # Print the first few rows of df_update1 for verification
                        print("\nFirst rows of df_update1 (products with new vendors):")
                        if len(df_update1) > 0:
                            sample_size = min(5, len(df_update1))
                            for i in range(sample_size):
                                row = df_update1.iloc[i]
                                print(f"  {i+1}. Product: {row['default_code']}, Name: {row['name'][:30]}..., "
                                      f"Supplier: {row['seller_ids']}, Price: {row['seller_ids/price']}")
                    else:
                        # Jika tidak ada baris yang difilter, buat DataFrame kosong
                        print("\nNo rows selected for update - all suppliers already registered for their products")
                        old_df = df_update1.copy()
                        df_update1 = pd.DataFrame(columns=['id', 'default_code', 'name', 'seller_ids', 'seller_ids/price', 'seller_ids/x_studio_so_number'])
                
                else:
                    print("\nNo valid default_codes found to search in Odoo")
                    
                    # Keep all rows since we can't verify against Odoo
                    print("Keeping all rows since Odoo validation couldn't be performed")
                    # Print the first few rows of df_update1 for verification
                    if len(df_update1) > 0:
                        sample_size = min(5, len(df_update1))
                        print(f"\nFirst {sample_size} rows of df_update1:")
                        for i in range(sample_size):
                            row = df_update1.iloc[i]
                            print(f"  {i+1}. Product: {row['default_code']}, Name: {row['name'][:30]}..., "
                                  f"Supplier: {row['seller_ids']}, Price: {row['seller_ids/price']}")
        except Exception as e:
            print(f"Error connecting to Odoo: {str(e)}")
            import traceback
            print(traceback.format_exc())
    else:
        print("Odoo connection is disabled in config.py. Set 'enabled': True to connect.")
        
        # Odoo is disabled, keep all rows as is
        print("\nOdoo connection disabled, keeping all rows in df_update1")
        # Print the first few rows of df_update1 for verification
        if len(df_update1) > 0:
            sample_size = min(5, len(df_update1))
            print(f"\nFirst {sample_size} rows of df_update1:")
            for i in range(sample_size):
                row = df_update1.iloc[i]
                print(f"  {i+1}. Product: {row['default_code']}, Name: {row['name'][:30]}..., "
                      f"Supplier: {row['seller_ids']}, Price: {row['seller_ids/price']}")
    
    # Clear NA values or convert to empty string as needed
    import streamlit as st
    if 'so_number' in st.session_state:
        df_update1['seller_ids/x_studio_so_number'] = st.session_state.so_number
    else:
        df_update1['seller_ids/x_studio_so_number'] = ""
    df_update1 = df_update1.fillna('')
    
    # Check for products with empty ID and add them to the list
    empty_id_rows = df_update1[df_update1['id'] == '']
    
    if not empty_id_rows.empty:
        for idx, row in empty_id_rows.iterrows():
            products_with_empty_id.append({
                'default_code': row['default_code'],
                'name': row['name'],
                'id': 'KOSONG',
                'status': 'ID Tidak Ditemukan'
            })
        print(f"\nAda {len(products_with_empty_id)} produk yang tidak ditemukan ID-nya di Odoo")
    
    # Process the second table
    # Call the function from df_UpdateProduct2.py to process the second table
    process_second_table(df_filtered, odoo_data)
    
    # Check final result
    print("\nFinal df_update1 summary:")
    print(f"Shape: {df_update1.shape}")
    print("Non-empty values by column:")
    for col in df_update1.columns:
        non_empty = (df_update1[col] != '').sum()
        print(f"  - '{col}': {non_empty} non-empty values")
    
    # Ensure columns are in the exact order specified
    df_update1 = df_update1[['id', 'default_code', 'name', 'seller_ids', 'seller_ids/price', 'seller_ids/x_studio_so_number']]
    
    # Save to Excel
    save_to_excel(df_update1)
    
    return df_update1

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

def save_to_excel(df=None):
    """
    Save df_update1 to Excel file.
    
    Args:
        df (pandas.DataFrame, optional): DataFrame to save. If None, transform() will be called.
        
    Returns:
        str: Path to saved Excel file
    """
    if df is None or df.empty:
        print("No data to save")
        return None
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(OUTPUT_PATHS['UpdateProduct']), exist_ok=True)
    
    # Save to Excel
    try:
        df.to_excel(OUTPUT_PATHS['UpdateProduct'], index=False)
        print(f"DataFrame saved to {OUTPUT_PATHS['UpdateProduct']}")
        return OUTPUT_PATHS['UpdateProduct']
    except Exception as e:
        print(f"Error saving to Excel: {str(e)}")
        return None

def get_missing_xml_ids():
    """
    Mengembalikan daftar produk yang tidak memiliki XML ID atau memiliki ID kosong.
    
    Returns:
        list: Daftar dictionary dengan informasi produk
    """
    global products_missing_xml_id, products_with_empty_id
    
    # Gabungkan kedua daftar produk
    all_problematic_products = products_missing_xml_id + products_with_empty_id
    
    return all_problematic_products

def load_from_excel():
    """
    Load UpdateProduct data from Excel file.
    
    Returns:
        pandas.DataFrame: DataFrame loaded from Excel
    """
    try:
        if os.path.exists(OUTPUT_PATHS['UpdateProduct']):
            return pd.read_excel(OUTPUT_PATHS['UpdateProduct'])
        else:
            print(f"File not found: {OUTPUT_PATHS['UpdateProduct']}")
            return None
    except Exception as e:
        print(f"Error loading from Excel: {str(e)}")
        return None

if __name__ == "__main__":
    # Test the function
    df_update1 = transform()
    print("Transform complete")
