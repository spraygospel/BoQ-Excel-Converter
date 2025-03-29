import pandas as pd
import pickle
import os
from config import TEMP_PATH, OUTPUT_PATHS

# Path untuk menyimpan hasil transformasi df_SalesOrder
DF_SO_PATH = os.path.join(TEMP_PATH, "df_so.pkl")

def transform():
    """
    Transform data dari df_base.pkl menjadi format Sales Order
    dan simpan hasilnya ke df_so.pkl dan Excel
    
    Returns:
        DataFrame: DataFrame hasil transformasi df_SalesOrder
    """
    # Load df_base
    df_base = load_df_base()
    
    if df_base is None:
        return None
    
    # Sortir df_base berdasarkan kolom Product
    if "Product" in df_base.columns:
        df_base_sorted = df_base.sort_values(by="Product").reset_index(drop=True)
    else:
        df_base_sorted = df_base.copy()
    
    # Dictionary untuk menyimpan total Line Total per Product
    product_totals = {}
    
    # Scan df_base dan hitung total per Product
    for _, row in df_base_sorted.iterrows():
        product = row.get("Product")
        line_total = row.get("Line Total")
        
        # Skip jika Product kosong atau None
        if product is None or pd.isna(product) or str(product).strip() == "":
            continue
            
        # Convert line_total ke float jika bisa, jika tidak gunakan 0
        try:
            if isinstance(line_total, str):
                # Bersihkan formatting seperti koma dan tanda mata uang
                line_total = line_total.replace(',', '').replace('.', '').strip()
                if line_total.isdigit():
                    line_total = float(line_total)
                else:
                    line_total = 0
            elif pd.isna(line_total):
                line_total = 0
            else:
                line_total = float(line_total)
        except:
            line_total = 0
            
        # Update dictionary
        if product in product_totals:
            product_totals[product] += line_total
        else:
            product_totals[product] = line_total
    
    # Buat DataFrame baru untuk Sales Order
    df_so = pd.DataFrame(columns=[
        "Product", "Description", "Ordered Qty", "Unit of Measure", "Unit Price", "Taxes"
    ])
    
    # Tambahkan data ke df_so berdasarkan product_totals
    for product, total in product_totals.items():
        df_so = pd.concat([df_so, pd.DataFrame([{
            "Product": product,
            "Description": product,  # Gunakan nama product sebagai description
            "Ordered Qty": 1,        # Qty selalu 1
            "Unit of Measure": "",   # Unit of Measure kosong
            "Unit Price": total,     # Unit Price adalah total dari Line Total
            "Taxes": "11% PPN Sale"  # Taxes selalu "11% PPN Sale"
        }])], ignore_index=True)
    
    # Langkah kedua: Tambahkan data dari Single Product yang tidak NULL
    for _, row in df_base_sorted.iterrows():
        single_product = row.get("Single Product")
        
        # Skip jika Single Product kosong atau None
        if single_product is None or pd.isna(single_product) or str(single_product).strip() == "":
            continue
            
        # Ambil data yang diperlukan
        desc = row.get("Description - Item yang Ditawarkan", "")
        # Tambahkan prefix "V - " jika belum ada
        if not pd.isna(desc) and not str(desc).strip().startswith("V - "):
            desc = "V - " + str(desc).strip()
        jumlah = row.get("Qty.", 0)
        uPrice = row.get("Unit Price", 0)
        
        # Convert jumlah dan uPrice ke tipe numerik jika perlu
        try:
            if isinstance(jumlah, str):
                jumlah = jumlah.replace(',', '').replace('.', '').strip()
                if jumlah.isdigit():
                    jumlah = float(jumlah)
                else:
                    jumlah = 0
            elif pd.isna(jumlah):
                jumlah = 0
            else:
                jumlah = float(jumlah)
        except:
            jumlah = 0
            
        try:
            if isinstance(uPrice, str):
                uPrice = uPrice.replace(',', '').replace('.', '').strip()
                if uPrice.isdigit():
                    uPrice = float(uPrice)
                else:
                    uPrice = 0
            elif pd.isna(uPrice):
                uPrice = 0
            else:
                uPrice = float(uPrice)
        except:
            uPrice = 0
        
        # Tambahkan data ke df_so
        df_so = pd.concat([df_so, pd.DataFrame([{
            "Product": desc,
            "Description": desc,
            "Ordered Qty": jumlah,
            "Unit of Measure": "",
            "Unit Price": uPrice,
            "Taxes": "11% PPN Sale"
        }])], ignore_index=True)
    
    # Pastikan direktori temp ada
    os.makedirs(TEMP_PATH, exist_ok=True)
    
    # Simpan df_so ke pickle
    with open(DF_SO_PATH, 'wb') as f:
        pickle.dump(df_so, f)
    
    # Simpan df_so ke Excel
    df_so.to_excel(OUTPUT_PATHS["SalesOrder"], index=False)
    
    return df_so

def load_df_base():
    """
    Load df_base dari pickle file
    
    Returns:
        DataFrame: df_base atau None jika file tidak ada
    """
    df_base_path = os.path.join(TEMP_PATH, "df_base.pkl")
    try:
        if os.path.exists(df_base_path):
            with open(df_base_path, 'rb') as f:
                return pickle.load(f)
        return None
    except Exception as e:
        print(f"Error loading df_base: {str(e)}")
        return None

def load_data():
    """
    Load data dari pickle file df_so.pkl
    
    Returns:
        DataFrame: df_so atau None jika file tidak ada
    """
    try:
        if os.path.exists(DF_SO_PATH):
            with open(DF_SO_PATH, 'rb') as f:
                return pickle.load(f)
        return None
    except Exception as e:
        print(f"Error loading df_so: {str(e)}")
        return None

# Alias untuk backward compatibility
transform_and_save = transform
