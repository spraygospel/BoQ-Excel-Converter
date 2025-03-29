import pandas as pd
import numpy as np
from ETL_library.validate import CrossFileValidator

def validate(boq_df, so_df, boq_validation_col, so_validation_col):
    """
    Validasi data antara DataFrame BoQ dan Convert to SO menggunakan logika baru.
    Juga melakukan reorganisasi df_so untuk memindahkan Single Product ke bagian bawah.
    
    Args:
        boq_df (pd.DataFrame): DataFrame dari file BoQ
        so_df (pd.DataFrame): DataFrame dari file Convert to SO
        boq_validation_col (str): Nama kolom validasi di BoQ
        so_validation_col (str): Nama kolom validasi di Convert to SO
        
    Returns:
        dict: Dictionary dengan hasil validasi:
            - "potential_mismatch": List data yang tidak cocok
            - "items": List data yang teridentifikasi sebagai items/kategori
            - "single_product": List data yang teridentifikasi sebagai produk tunggal
            - "boq_df": DataFrame BoQ yang sudah diproses
            - "so_df": DataFrame Convert to SO yang sudah diproses dan direorganisasi
    """
    # Cek keberadaan kolom
    if boq_validation_col not in boq_df.columns:
        raise ValueError(f"Kolom '{boq_validation_col}' tidak ditemukan di BoQ DataFrame")
    if so_validation_col not in so_df.columns:
        raise ValueError(f"Kolom '{so_validation_col}' tidak ditemukan di Convert to SO DataFrame")
    
    # Inisialisasi dictionary hasil
    result = {
        "potential_mismatch": [],
        "items": [],
        "single_product": []
    }
    
    # 1. Jalankan CrossFileValidator untuk mencari ketidakcocokan
    validator = CrossFileValidator(error_handling="warn")
    is_match, validation_report = validator.validate_matching_values(
        first_df=boq_df,
        second_df=so_df,
        first_col=boq_validation_col,
        second_col=so_validation_col,
        case_sensitive=False,
        label_first="BoQ",
        label_second="Convert to SO"
    )
    
    # 2. Dapatkan data yang tidak cocok (potential mismatch)
    potential_mismatch = []
    if 'validation_type' in validation_report.columns and 'value' in validation_report.columns:
        mismatch_df = validation_report[validation_report['validation_type'] == 'unmatched_value']
        
        for _, row in mismatch_df.iterrows():
            potential_mismatch.append({
                'source': row.get('file', ''),
                'value': row.get('value', ''),
                'column': row.get('column', '')
            })
    
    # Simpan potential_mismatch awal
    result["potential_mismatch"] = potential_mismatch.copy()
    
    # 3. Identifikasi Items (kategori) dari potential_mismatch yang berasal dari BoQ
    items_to_remove = []
    for i, item in enumerate(potential_mismatch):
        if item['source'] == 'BoQ' and item['column'] == boq_validation_col:
            value = item['value']
            
            # Cari row di boq_df yang memiliki nilai tersebut
            rows = boq_df.index[boq_df[boq_validation_col].astype(str).str.lower() == value.lower()].tolist()
            
            if rows:
                row_index = rows[0]
                row_data = boq_df.iloc[row_index]
                
                # Cek apakah sel di kiri & kanan kosong
                left_values = []
                right_values = []
                
                # Ambil nilai di kiri & kanan dari Description - Item yang Ditawarkan
                desc_idx = list(boq_df.columns).index(boq_validation_col)
                
                # Cek kolom di kiri (jika bukan kolom pertama)
                if desc_idx > 0:
                    left_col = boq_df.columns[desc_idx - 1]
                    left_val = row_data[left_col]
                    if pd.isna(left_val) or (isinstance(left_val, str) and left_val.strip() == ''):
                        left_values.append(True)  # Kosong
                    else:
                        left_values.append(False)  # Tidak kosong
                
                # Cek kolom di kanan (jika bukan kolom terakhir)
                if desc_idx < len(boq_df.columns) - 1:
                    right_col = boq_df.columns[desc_idx + 1]
                    right_val = row_data[right_col]
                    if pd.isna(right_val) or (isinstance(right_val, str) and right_val.strip() == ''):
                        right_values.append(True)  # Kosong
                    else:
                        right_values.append(False)  # Tidak kosong
                
                # Jika semua kiri dan semua kanan kosong, identifikasi sebagai Item
                if all(left_values) and all(right_values):
                    result["items"].append({
                        'source': 'BoQ',
                        'value': value,
                        'column': boq_validation_col,
                        'row_index': row_index
                    })
                    items_to_remove.append(i)
    
    # Hapus items dari potential_mismatch
    for i in sorted(items_to_remove, reverse=True):
        potential_mismatch.pop(i)
    
    # 4. Identifikasi Single Product dari potential_mismatch yang berasal dari BoQ
    single_products_to_remove = []
    for i, item in enumerate(potential_mismatch):
        if item['source'] == 'BoQ' and item['column'] == boq_validation_col:
            value = item['value']
            
            # Cari row di boq_df yang memiliki nilai tersebut
            rows = boq_df.index[boq_df[boq_validation_col].astype(str).str.lower() == value.lower()].tolist()
            
            if rows:
                row_index = rows[0]
                row_data = boq_df.iloc[row_index]
                
                # Cek apakah sel di kiri ATAU di kanan tidak kosong
                left_empty = True
                right_empty = True
                
                # Ambil nilai di kiri & kanan dari Description - Item yang Ditawarkan
                desc_idx = list(boq_df.columns).index(boq_validation_col)
                
                # Cek kolom di kiri (jika bukan kolom pertama)
                if desc_idx > 0:
                    left_col = boq_df.columns[desc_idx - 1]
                    left_val = row_data[left_col]
                    if not (pd.isna(left_val) or (isinstance(left_val, str) and left_val.strip() == '')):
                        left_empty = False  # Tidak kosong
                
                # Cek kolom di kanan (jika bukan kolom terakhir)
                if desc_idx < len(boq_df.columns) - 1:
                    right_col = boq_df.columns[desc_idx + 1]
                    right_val = row_data[right_col]
                    if not (pd.isna(right_val) or (isinstance(right_val, str) and right_val.strip() == '')):
                        right_empty = False  # Tidak kosong
                
                # Jika kiri ATAU kanan tidak kosong
                if not (left_empty and right_empty):
                    # Cek keberadaan di kolom "Product" di so_df
                    if 'Product' in so_df.columns:
                        product_match = so_df['Product'].astype(str).str.lower() == value.lower()
                        if product_match.any():
                            result["single_product"].append({
                                'source': 'BoQ',
                                'value': value,
                                'column': boq_validation_col,
                                'row_index': row_index,
                                'action': 'Move to bottom'
                            })
                            single_products_to_remove.append(i)
    
    # Hapus single products dari potential_mismatch
    for i in sorted(single_products_to_remove, reverse=True):
        if i < len(potential_mismatch):  # Safety check
            potential_mismatch.pop(i)
    
    # 5. Cek produk di Convert to SO yang berisi Product tapi BOM Line kosong
    if 'Product' in so_df.columns:
        for idx, row in so_df.iterrows():
            product_val = row.get('Product')
            bom_line_val = row.get(so_validation_col)
            
            # Jika Product berisi nilai tapi BOM Line kosong
            if (not pd.isna(product_val) and isinstance(product_val, str) and product_val.strip() != '' and 
                (pd.isna(bom_line_val) or (isinstance(bom_line_val, str) and bom_line_val.strip() == ''))):
                
                # Cek keberadaan di BoQ
                match_in_boq = boq_df[boq_validation_col].astype(str).str.lower() == product_val.lower()
                if match_in_boq.any():
                    # Cari di potential_mismatch
                    found = False
                    for i, item in enumerate(potential_mismatch):
                        if (item['source'] == 'BoQ' and 
                            item['column'] == boq_validation_col and 
                            item['value'].lower() == product_val.lower()):
                            
                            result["single_product"].append({
                                'source': 'Convert to SO',
                                'value': product_val,
                                'column': 'Product',
                                'row_index': idx,
                                'action': 'Move to bottom'
                            })
                            potential_mismatch.pop(i)
                            found = True
                            break
                    
                    if not found:
                        # Tambahkan ke single_product langsung
                        result["single_product"].append({
                            'source': 'Convert to SO',
                            'value': product_val,
                            'column': 'Product',
                            'row_index': idx,
                            'action': 'Move to bottom'
                        })
    
    # Update potential_mismatch final
    result["potential_mismatch"] = potential_mismatch
    
    # 6. Reorganisasi df_so untuk memindahkan Single Product ke bagian bawah
    # Salin DataFrame asli
    processed_so_df = so_df.copy()
    
    # Identifikasi row yang perlu dipindahkan ke bawah (Single Product)
    rows_to_move = []
    for item in result["single_product"]:
        if item['source'] == 'Convert to SO' and item['action'] == 'Move to bottom':
            rows_to_move.append(item['row_index'])
    
    # Jika ada row yang perlu dipindahkan
    if rows_to_move:
        # Ambil row yang akan dipindahkan
        move_rows_data = processed_so_df.loc[rows_to_move].copy()
        
        # Hapus row tersebut dari posisi asli
        processed_so_df = processed_so_df.drop(rows_to_move)
        
        # Tambahkan kembali sebagai row baru di bagian bawah
        processed_so_df = pd.concat([processed_so_df, move_rows_data], ignore_index=True)
    
    # 7. Hapus row kosong di df_so
    if not processed_so_df.empty:
        # Cek apakah row kosong atau tidak
        is_empty_row = processed_so_df.isna().all(axis=1) | (processed_so_df.astype(str).apply(lambda x: x.str.strip() == '').all(axis=1))
        
        # Filter hanya row yang tidak kosong
        processed_so_df = processed_so_df[~is_empty_row].reset_index(drop=True)
    
    # 8. Validasi Unit of Measure
    # Inisialisasi list untuk menyimpan anomali Unit of Measure
    result["uom_anomalies"] = []
    
    if "Unit of Measure" in processed_so_df.columns:
        # Cek kolom Product dan BOM Line
        product_col = next((col for col in processed_so_df.columns if col.lower() == "product"), None)
        bom_line_col = so_validation_col
        
        for idx, row in processed_so_df.iterrows():
            uom_value = row.get("Unit of Measure")
            product_value = row.get(product_col) if product_col else None
            bom_line_value = row.get(bom_line_col) if bom_line_col else None
            
            # Case 1: UoM ada tapi Product ATAU BOM Line kosong
            if not pd.isna(uom_value) and isinstance(uom_value, str) and uom_value.strip() != "":
                # Cek apakah ada nilai di Product dan BOM Line
                product_empty = pd.isna(product_value) or (isinstance(product_value, str) and product_value.strip() == "")
                bom_line_empty = pd.isna(bom_line_value) or (isinstance(bom_line_value, str) and bom_line_value.strip() == "")
                
                if product_empty or bom_line_empty:
                    anomaly = {
                        'row_index': idx,
                        'uom_value': uom_value,
                        'product_value': product_value if not product_empty else None,
                        'bom_line_value': bom_line_value if not bom_line_empty else None,
                        'issue': "UoM exists but Product or BOM Line is missing",
                        'fields_missing': "Product" if product_empty else "" + 
                                         ("BOM Line" if bom_line_empty else "")
                    }
                    
                    result["uom_anomalies"].append(anomaly)
                    
                    # Tambahkan juga ke potential_mismatch
                    result["potential_mismatch"].append({
                        'source': 'Convert to SO',
                        'value': f"Row {idx+1}: UoM '{uom_value}' exists but {'Product' if product_empty else 'BOM Line'} is missing",
                        'column': 'Unit of Measure',
                        'type': 'UoM Anomaly'
                    })
            
            # Case 2: Product dan BOM Line ada tapi UoM kosong
            elif (not pd.isna(product_value) and isinstance(product_value, str) and product_value.strip() != "") and \
                 (not pd.isna(bom_line_value) and isinstance(bom_line_value, str) and bom_line_value.strip() != ""):
                
                # Cek apakah UoM kosong
                if pd.isna(uom_value) or (isinstance(uom_value, str) and uom_value.strip() == ""):
                    anomaly = {
                        'row_index': idx,
                        'uom_value': None,
                        'product_value': product_value,
                        'bom_line_value': bom_line_value,
                        'issue': "Product and BOM Line exist but UoM is missing",
                        'fields_missing': "Unit of Measure"
                    }
                    
                    result["uom_anomalies"].append(anomaly)
                    
                    # Tambahkan juga ke potential_mismatch
                    result["potential_mismatch"].append({
                        'source': 'Convert to SO',
                        'value': f"Row {idx+1}: Product '{product_value}' and BOM Line '{bom_line_value}' exist but UoM is missing",
                        'column': 'Unit of Measure',
                        'type': 'UoM Anomaly'
                    })

    # Tambahkan DataFrame yang sudah diproses ke hasil
    result["boq_df"] = boq_df.copy()
    result["so_df"] = processed_so_df
    
    return result
