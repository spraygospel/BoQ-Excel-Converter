import pandas as pd
import os
import pickle
from config import OUTPUT_PATHS, BOQ_VALIDATION_COL
from df_transformation.df_base import load_df_base

def transform():
    """
    Transform data dari df_base menjadi format ProductVariant sesuai aturan yang ditentukan
    
    - Mengambil data dari df_base
    - Menerapkan aturan transformasi untuk setiap kolom
    - Menghapus baris yang memiliki "Internal References"
    
    Returns:
        pd.DataFrame: DataFrame hasil transformasi dalam format ProductVariant
    """
    # Load data dari df_base
    try:
        df_base = load_df_base()
        if df_base is None:
            print("Error: df_base is None")
            # Return dummy data jika df_base tidak tersedia
            return create_dummy_product_variant()
    except Exception as e:
        print(f"Error loading df_base: {str(e)}")
        # Return dummy data jika terjadi error
        return create_dummy_product_variant()
    
    # Debug: Print kolom-kolom yang ada di df_base
    print(f"Columns in df_base: {df_base.columns.tolist()}")
    
    # Print beberapa baris pertama untuk debugging
    print("Sample rows from df_base:")
    print(df_base.head(2).to_string())
    
    # Buat DataFrame baru dengan kolom yang diinginkan
    columns = [
        "Sequence", 
        "Name", 
        "Product Type", 
        "Product Type Info", 
        "Vendors/Display Name", 
        "Vendors/Price", 
        "Vendors/Company", 
        "Cost", 
        "Public Price", 
        "Unit of Measure", 
        "Purchase Unit of Measure", 
        "Sales Description", 
        "Purchase Description", 
        "Item"
    ]
    
    # Inisialisasi DataFrame kosong
    df_product = pd.DataFrame(columns=columns)
    
    # Ambil company dari pickle jika ada
    company = get_company()
    
    # Cek dan filter baris yang memiliki "Internal References"
    print("Checking for rows with Internal References...")
    df_base_filtered = []
    internal_ref_col = find_column(df_base, "Internal References")
    if internal_ref_col:
        print(f"Found 'Internal References' column: {internal_ref_col}")
        # Tampilkan sample nilai dari kolom Internal References
        print("Sample Internal References values:")
        print(df_base[internal_ref_col].head(10).to_list())
        
        # Lakukan filter baris per baris, hanya ambil yang tidak punya Internal References
        skipped_count = 0
        for idx, row in df_base.iterrows():
            internal_ref = row[internal_ref_col]
            if pd.isna(internal_ref) or (isinstance(internal_ref, str) and internal_ref.strip() == ""):
                df_base_filtered.append(row)
            else:
                skipped_count += 1
                if skipped_count <= 5:  # Hanya print 5 pertama
                    print(f"Skipping row with Internal Reference: {internal_ref}")
        
        print(f"Filtered out {skipped_count} rows with Internal References")
        # Convert list of rows to DataFrame
        if df_base_filtered:
            df_base_filtered = pd.DataFrame(df_base_filtered)
        else:
            print("Warning: All rows were filtered out!")
            df_base_filtered = pd.DataFrame(columns=df_base.columns)
    else:
        print("No 'Internal References' column found, using all rows")
        df_base_filtered = df_base
    
    print(f"Original df_base: {len(df_base)} rows, Filtered: {len(df_base_filtered)} rows")
    
    # Isi DataFrame dengan data dari df_base_filtered sesuai aturan yang ditentukan
    rows = []
    for idx, row in df_base_filtered.iterrows():
        new_row = {}
        
        # Inisialisasi new_row (hapus Sequence karena akan diisi di akhir)
        new_row = {}
        
        # 2. "Name" berisi data dari kolom "Description - Item yang Ditawarkan"
        description_col = find_column(df_base, BOQ_VALIDATION_COL)
        if description_col:
            new_row["Name"] = row[description_col]
        else:
            new_row["Name"] = f"Product {idx+1}"
        
        # 3 & 4. "Product Type" & "Product Type Info" berdasarkan kolom "VN"
        vn_col = find_column(df_base, "VN")
        if vn_col and pd.notna(row[vn_col]):
            vn_value = str(row[vn_col]).lower().strip()
            if vn_value == "yes":
                new_row["Product Type"] = "Consumable"
                new_row["Product Type Info"] = "Consumable"
            else:
                new_row["Product Type"] = "Storable Product"
                new_row["Product Type Info"] = "Storable Product"
        else:
            new_row["Product Type"] = "Storable Product"
            new_row["Product Type Info"] = "Storable Product"
        
        # 5. "Vendors/Display Name" dari kolom "Supplier"
        supplier_col = find_column(df_base, "Supplier")
        if supplier_col and pd.notna(row[supplier_col]):
            new_row["Vendors/Display Name"] = row[supplier_col]
        else:
            new_row["Vendors/Display Name"] = ""
        
        # 6. "Vendors/Price" & "Cost" dari kolom "Modal Unit"
        # Coba dengan semua kemungkinan nama kolom untuk Modal Unit
        possible_modal_cols = ["Modal Unit", "Modal", "Unit Cost", "Unit Modal", "Cost per Unit", "Price Unit"]
        modal_col = None
        
        # Cari kolom yang sesuai
        for col_name in possible_modal_cols:
            found_col = find_column(df_base_filtered, col_name)
            if found_col:
                modal_col = found_col
                break
        
        # Debug: Tampilkan kolom yang ditemukan & nilainya
        if idx < 5:  # Hanya print untuk 5 baris pertama
            print(f"Row {idx}: Looking for Modal Unit, found column: {modal_col}")
            if modal_col:
                print(f"  Raw Value: '{row[modal_col]}' (type: {type(row[modal_col])})")
        
        modal_value = 0
        if modal_col and pd.notna(row[modal_col]):
            try:
                # Jika sudah numeric, gunakan langsung
                if isinstance(row[modal_col], (int, float)):
                    modal_value = float(row[modal_col])
                else:
                    # Jika string, bersihkan dan konversi
                    modal_str = str(row[modal_col])
                    # Hapus karakter non-numerik kecuali titik desimal
                    modal_str = ''.join(c for c in modal_str if c.isdigit() or c == '.')
                    if modal_str:
                        modal_value = float(modal_str)
                
                if idx < 5:
                    print(f"  Converted to: {modal_value}")
            except (ValueError, TypeError) as e:
                if idx < 5:
                    print(f"  Error converting: {e}")
        
        new_row["Vendors/Price"] = modal_value
        new_row["Cost"] = modal_value
        
        # 7. "Vendors/Company" dari pilihan dropdown company
        new_row["Vendors/Company"] = company if company else ""
        
        # 8. "Public Price" dari "Unit Price"
        price_col = find_column(df_base, "Unit Price")
        if price_col and pd.notna(row[price_col]):
            try:
                new_row["Public Price"] = float(row[price_col])
            except (ValueError, TypeError):
                new_row["Public Price"] = 0
        else:
            new_row["Public Price"] = 0
        
        # 9. "Unit of Measure" & "Purchase Unit of Measure" dari "UoM"
        uom_col = find_column(df_base, "UoM")
        if uom_col and pd.notna(row[uom_col]):
            new_row["Unit of Measure"] = row[uom_col]
            new_row["Purchase Unit of Measure"] = row[uom_col]
        else:
            new_row["Unit of Measure"] = "Units"
            new_row["Purchase Unit of Measure"] = "Units"
        
        # 10. "Sales Description" & "Purchase Description" dikosongkan
        new_row["Sales Description"] = ""
        new_row["Purchase Description"] = ""
        
        # 11. "Item" dari df_base
        item_col = find_column(df_base, "Item")
        if item_col and pd.notna(row[item_col]):
            new_row["Item"] = row[item_col]
        else:
            new_row["Item"] = ""
        
        # Tambahkan baris baru ke list
        rows.append(new_row)
    
    # Buat DataFrame dari list of dicts
    if rows:
        df_product = pd.DataFrame(rows, columns=columns[1:])  # Hapus Sequence dari columns
        
        # Tambahkan "V - " di depan setiap nilai di kolom "Name" jika belum ada
        if "Name" in df_product.columns:
            for idx, name in enumerate(df_product["Name"]):
                if pd.notna(name) and isinstance(name, str):
                    if not name.startswith("V - "):
                        df_product.at[idx, "Name"] = f"V - {name}"
        
        # Tambahkan kolom Sequence di awal dengan nilai berurutan
        df_product.insert(0, "Sequence", range(1, len(df_product) + 1))
    else:
        # Jika tidak ada data, gunakan data dummy
        print("No data processed from df_base, using dummy data")
        return create_dummy_product_variant()
    
    # Debug: Print info dasar
    print(f"df_product shape: {df_product.shape}")
    
    # Simpan hasil ke file Excel
    if not df_product.empty:
        save_to_excel(df_product)
    
    return df_product

def find_column(df, column_name):
    """
    Cari kolom dalam DataFrame dengan pencocokan case-insensitive
    
    Args:
        df (pd.DataFrame): DataFrame untuk dicari
        column_name (str): Nama kolom yang dicari
        
    Returns:
        str: Nama kolom asli jika ditemukan, None jika tidak
    """
    # 1. Exact match
    if column_name in df.columns:
        return column_name
    
    # 2. Case-insensitive exact match
    for col in df.columns:
        if col.lower() == column_name.lower():
            return col
    
    # 3. Substring match (untuk kolom dengan awalan/akhiran tambahan)
    for col in df.columns:
        if column_name.lower() in col.lower():
            return col
    
    # 4. Match dengan spasi fleksibel (misalnya "ModalUnit" untuk "Modal Unit")
    no_space_name = column_name.lower().replace(" ", "")
    for col in df.columns:
        no_space_col = col.lower().replace(" ", "")
        if no_space_name == no_space_col:
            return col
    
    return None

def get_company():
    """
    Ambil nilai company dari st.session_state atau config
    
    Returns:
        str: Nama company, atau string kosong jika tidak ditemukan
    """
    try:
        # Coba ambil langsung dari st.session_state jika tersedia
        import streamlit as st
        if 'company' in st.session_state:
            return st.session_state.company
        
        # Jika tidak berhasil, coba baca dari file session_state.pkl jika ada
        session_file = os.path.join("temp", "session_state.pkl")
        if os.path.exists(session_file):
            with open(session_file, 'rb') as f:
                session_data = pickle.load(f)
                if 'company' in session_data:
                    return session_data['company']
        
        # Jika tidak berhasil, coba baca dari config.py
        from config import DEFAULT_SETTINGS
        return DEFAULT_SETTINGS["company"]
    except Exception as e:
        print(f"Error getting company: {str(e)}")
        return ""

def create_dummy_product_variant():
    """Membuat DataFrame dummy untuk ProductVariant"""
    columns = [
        "Sequence", 
        "Name", 
        "Product Type", 
        "Product Type Info", 
        "Vendors/Display Name", 
        "Vendors/Price", 
        "Vendors/Company", 
        "Cost", 
        "Public Price", 
        "Unit of Measure", 
        "Purchase Unit of Measure", 
        "Sales Description", 
        "Purchase Description", 
        "Item"
    ]
    
    # Buat data dummy
    data = []
    for i in range(5):
        row = {
            "Name": f"V - Product {i+1}",  # Tambahkan "V - " di depan
            "Product Type": "Storable Product" if i % 2 == 0 else "Consumable",
            "Product Type Info": "Storable Product" if i % 2 == 0 else "Consumable",
            "Vendors/Display Name": f"Vendor {i+1}",
            "Vendors/Price": 100 * (i+1),
            "Vendors/Company": "PT. Visiniaga Mitra Kreasindo",
            "Cost": 80 * (i+1),
            "Public Price": 120 * (i+1),
            "Unit of Measure": "Units",
            "Purchase Unit of Measure": "Units",
            "Sales Description": "",
            "Purchase Description": "",
            "Item": f"ITEM-{i+1:03d}"
        }
        data.append(row)
    
    # Buat DataFrame dan tambahkan Sequence berurutan
    df = pd.DataFrame(data, columns=columns[1:])  # Tanpa Sequence
    df.insert(0, "Sequence", range(1, len(df) + 1))  # Tambahkan Sequence
    
    return df

def save_to_excel(df):
    """
    Simpan DataFrame hasil transformasi ke file Excel
    
    Args:
        df (pd.DataFrame): DataFrame untuk disimpan
        
    Returns:
        str: Path ke file Excel yang disimpan
    """
    output_path = OUTPUT_PATHS["ProductVariant"]
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df.to_excel(output_path, index=False)
    return output_path