import pandas as pd
import pickle
import os
from config import DF_BASE_PATH, BOQ_VALIDATION_COL, SO_VALIDATION_COL
from ETL_library.transform import EmptyspaceCleaner, DuplicateRestorer, SectionExtractor, DataFrameJoiner

# Definisi path untuk menyimpan df_base.pkl
DF_BASE_ONLY_PATH = os.path.join(os.path.dirname(DF_BASE_PATH), "df_base.pkl")

def transform_and_save(boq_df, so_df):
    """
    Transform dan simpan data dari kedua file input
    
    Args:
        boq_df (pd.DataFrame): DataFrame dari file BoQ
        so_df (pd.DataFrame): DataFrame dari file Convert to SO
        
    Returns:
        tuple: (boq_df, so_df) setelah transformasi
    """
    # Cek apakah dataframe ada
    if boq_df is None or so_df is None:
        raise ValueError("Both DataFrames must be provided")
    
    # Buat copy dari dataframe asli
    boq_df = boq_df.copy()
    so_df = so_df.copy()
    
    # Langkah 2: Bersihkan baris yang tidak perlu dengan EmptyspaceCleaner - hanya untuk df-BoQ
    
    # Validasi keberadaan kolom BOQ_VALIDATION_COL di boq_df
    boq_column = None
    for col in boq_df.columns:
        if col.lower() == BOQ_VALIDATION_COL.lower():
            boq_column = col
            break
    
    if boq_column:
        boq_cleaner = EmptyspaceCleaner(header_names=boq_column)
        boq_df = boq_cleaner.clean(boq_df)
    
    # Validasi keberadaan kolom SO_VALIDATION_COL di so_df
    so_column = None
    for col in so_df.columns:
        if col.lower() == SO_VALIDATION_COL.lower():
            so_column = col
            break
    
    # Langkah 3: Rename header di sebelah kanan header "Qty." pada "df-BoQ" menjadi "UoM"
    # Cari indeks kolom "Qty."
    qty_column = None
    qty_index = -1
    
    for i, col in enumerate(boq_df.columns):
        if col.lower() == "qty.":
            qty_column = col
            qty_index = i
            break
    
    # Jika menemukan kolom "Qty." dan ada kolom di sebelah kanannya
    if qty_index >= 0 and qty_index + 1 < len(boq_df.columns):
        # Ambil nama kolom setelah "Qty."
        next_column = boq_df.columns[qty_index + 1]
        # Buat daftar kolom baru
        new_columns = list(boq_df.columns)
        # Ganti nama kolom setelah "Qty." menjadi "UoM"
        new_columns[qty_index + 1] = "UoM"
        # Terapkan daftar kolom baru
        boq_df.columns = new_columns
    
    # Langkah 4: Gunakan DuplicateRestorer untuk kolom "Product" pada so_df
    # Validasi keberadaan kolom "Product" di so_df
    product_column = None
    for col in so_df.columns:
        if col.lower() == "product":
            product_column = col
            break

    # Validasi keberadaan kolom "Unit of Measure" di so_df
    uom_column = None
    for col in so_df.columns:
        if col.lower() == "unit of measure":
            uom_column = col
            break

    if product_column:
        product_restorer = DuplicateRestorer(columns_to_restore=product_column)
        so_df = product_restorer.transform(so_df)

    # Kondisi khusus untuk DuplicateRestorer pada Unit of Measure
    if uom_column and so_column:
        # Identifikasi rows di mana BOM Line kosong tapi Product berisi data
        # Untuk rows tersebut, jangan restore UoM
        mask_skip_restore = pd.Series(False, index=so_df.index)
        
        for idx, row in so_df.iterrows():
            product_val = row.get(product_column, None) if product_column else None
            bom_val = row.get(so_column, None)
            
            # Check if Product has value but BOM Line is empty
            if (pd.notna(product_val) and (not isinstance(product_val, str) or product_val.strip() != "")) and \
            (pd.isna(bom_val) or (isinstance(bom_val, str) and bom_val.strip() == "")):
                mask_skip_restore[idx] = True
        
        # Jalankan DuplicateRestorer pada rows yang tidak di-skip
        if not mask_skip_restore.all():
            # Simpan nilai-nilai original untuk rows yang di-skip
            original_uom_values = so_df.loc[mask_skip_restore, uom_column].copy()
            
            # Jalankan DuplicateRestorer pada seluruh dataframe
            uom_restorer = DuplicateRestorer(columns_to_restore=uom_column)
            so_df = uom_restorer.transform(so_df)
            
            # Kembalikan nilai original untuk rows yang di-skip
            so_df.loc[mask_skip_restore, uom_column] = original_uom_values
    
    # Langkah 5: Gunakan SectionExtractor untuk df-BoQ
    if boq_column:
        section_extractor = SectionExtractor(
            section_indicator_col=boq_column,
            target_section_col="Item",
            remove_section_rows=True
        )
        boq_df = section_extractor.extract(boq_df)
    
    # Langkah 6: Gunakan DataFrameJoiner untuk menggabungkan df-SO ke df-BoQ
    if boq_column and so_column:
        # Validasi keberadaan kolom "Product", "VN", dan "Unit of Measure" di so_df
        product_exists = any(col.lower() == "product" for col in so_df.columns)
        vn_exists = any(col.lower() == "vn" for col in so_df.columns)
        uom_exists = any(col.lower() == "unit of measure" for col in so_df.columns)
        
        columns_to_add = []
        
        if product_exists:
            columns_to_add.append(next(col for col in so_df.columns if col.lower() == "product"))
        
        if vn_exists:
            columns_to_add.append(next(col for col in so_df.columns if col.lower() == "vn"))
        
        if uom_exists:
            columns_to_add.append(next(col for col in so_df.columns if col.lower() == "unit of measure"))
        
        if columns_to_add:
            joiner = DataFrameJoiner(
                left_key=boq_column,
                right_key=so_column,
                join_type="left",
                columns_to_add=columns_to_add,
                match_case=False
            )
            boq_df = joiner.join(boq_df, so_df)
            
            # Pastikan kolom "Unit of Measure" ada di paling kanan
            if uom_exists and "Unit of Measure" in boq_df.columns:
                # Simpan Unit of Measure
                uom_column = next(col for col in boq_df.columns if col.lower() == "unit of measure")
                uom_values = boq_df[uom_column].copy()
                
                # Hapus kolom asli
                boq_df = boq_df.drop(columns=[uom_column])
                
                # Tambahkan kembali di posisi terakhir
                boq_df["Unit of Measure"] = uom_values
    
    # Langkah 7: Tambahkan field "Single Product" di df-BoQ
    # Tambahkan kolom "Single Product" yang awalnya kosong
    boq_df["Single Product"] = None
    
    # Dapatkan kolom Product dari df-SO
    product_column_name = next((col for col in so_df.columns if col.lower() == "product"), None)
    
    if boq_column and so_column and product_column_name:
        # Cari sel kosong di kolom "BOM Line" di df-SO
        for idx, row in so_df.iterrows():
            bom_line_value = row[so_column]
            
            # Jika BOM Line kosong, ambil nilai Product sebagai single product
            if pd.isna(bom_line_value) or (isinstance(bom_line_value, str) and bom_line_value.strip() == ""):
                temp_single_product = row[product_column_name]
                
                # Jika temp_single_product tidak kosong
                if pd.notna(temp_single_product) and (not isinstance(temp_single_product, str) or temp_single_product.strip() != ""):
                    # Cari di df-BoQ, di kolom description mana yang nilainya sama dengan temp_single_product
                    for boq_idx, boq_row in boq_df.iterrows():
                        boq_desc_value = boq_row[boq_column]
                        
                        # Bandingkan nilai (case insensitive)
                        if (pd.notna(boq_desc_value) and 
                            isinstance(boq_desc_value, str) and 
                            isinstance(temp_single_product, str) and
                            boq_desc_value.lower().strip() == temp_single_product.lower().strip()):
                            
                            # Jika ketemu, isi nilai "Single Product"
                            boq_df.at[boq_idx, "Single Product"] = temp_single_product
    
    # Pastikan direktori temp ada
    os.makedirs(os.path.dirname(DF_BASE_PATH), exist_ok=True)
    
    # Simpan kedua DataFrame ke pickle file
    with open(DF_BASE_PATH, 'wb') as f:
        pickle.dump({
            'boq_df': boq_df,
            'so_df': so_df
        }, f)
    
    # Simpan hanya boq_df sebagai df_base.pkl untuk digunakan oleh modul transformasi lainnya
    with open(DF_BASE_ONLY_PATH, 'wb') as f:
        pickle.dump(boq_df, f)
    
    return boq_df, so_df

def load_data():
    """
    Load data dari pickle file
    
    Returns:
        tuple: (boq_df, so_df) atau (None, None) jika file tidak ada
    """
    try:
        if os.path.exists(DF_BASE_PATH):
            with open(DF_BASE_PATH, 'rb') as f:
                data = pickle.load(f)
                return data['boq_df'], data['so_df']
        return None, None
    except Exception as e:
        print(f"Error loading data: {str(e)}")
        return None, None

def load_df_base():
    """
    Load hanya df_base dari pickle file
    
    Returns:
        DataFrame: df_base atau None jika file tidak ada
    """
    try:
        if os.path.exists(DF_BASE_ONLY_PATH):
            with open(DF_BASE_ONLY_PATH, 'rb') as f:
                return pickle.load(f)
        return None
    except Exception as e:
        print(f"Error loading df_base: {str(e)}")
        return None
