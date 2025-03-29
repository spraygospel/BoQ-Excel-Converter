import pandas as pd
import pickle
import os
from config import TEMP_PATH, OUTPUT_PATHS
from ETL_library.transform import DuplicateSuppressor

def transform():
    """
    Transform base dataframe into Bill of Material format
    
    Returns:
        DataFrame: Transformed BOM dataframe
    """
    # Path untuk df_base.pkl yang hanya berisi boq_df
    df_base_only_path = os.path.join(TEMP_PATH, "df_base.pkl")
    
    # Path untuk df_base.pkl yang berisi dictionary
    df_base_path = os.path.join(os.path.dirname(TEMP_PATH), "df_base.pkl")
    
    # Coba load dari df_base_only_path dulu
    df_base = None
    try:
        if os.path.exists(df_base_only_path):
            with open(df_base_only_path, 'rb') as f:
                df_base = pickle.load(f)
        # Jika tidak ada, coba load dari df_base_path
        elif os.path.exists(df_base_path):
            with open(df_base_path, 'rb') as f:
                data = pickle.load(f)
                if isinstance(data, dict) and 'boq_df' in data:
                    df_base = data['boq_df']
                else:
                    df_base = data
        
        if df_base is None or not isinstance(df_base, pd.DataFrame) or df_base.empty:
            print("Error: df_base is None or empty")
            # Return empty DataFrame with correct columns
            return pd.DataFrame(columns=[
                'Sequence', 'Product', 'Product Variant', 'Quantity', 
                'BoM Type', 'Company', 'Unit of Measure', 'BoM Lines/Component', 'BoM Lines/Quantity'
            ])
            
        # Simpan salinan yang terurut
        df_base_sorted = df_base.copy()
        
        # Cek apakah kolom 'Product' ada
        if 'Product' in df_base_sorted.columns:
            # Sort berdasarkan kolom Product
            df_base_sorted = df_base_sorted.sort_values(by='Product', ascending=True)
        
        # Inisialisasi df_bom
        df_bom = pd.DataFrame(columns=[
            'Sequence', 'Product', 'Product Variant', 'Quantity', 
            'BoM Type', 'Company', 'BoM Lines/Component', 'BoM Lines/Quantity'
        ])
        
        # Proses untuk mengisi df_bom
        rows = []
        
        # Get company dari session state atau gunakan default
        try:
            import streamlit as st
            company = st.session_state.get('company', "PT. Visiniaga Mitra Kreasindo")
        except:
            company = "PT. Visiniaga Mitra Kreasindo"
        
        # Dictionary untuk melacak sequence berdasarkan Product Variant
        product_variant_seq = {}
        current_seq = 1
        
        # Loop untuk mengisi data - pastikan data dikelompokkan dengan benar untuk DuplicateSuppressor
        df_base_sorted = df_base_sorted.sort_values(by='Product') if 'Product' in df_base_sorted.columns else df_base_sorted
        
        current_product = None
        sequence = 0
        
        # Loop untuk mengisi data
        for idx, row in df_base_sorted.iterrows():
            product = row.get('Product', '')
            if pd.isna(product) or product == '':
                continue
                
            # Update sequence saat product berubah
            if current_product != product:
                current_product = product
                sequence += 1
            
            # Get component value (Description - Item yang Ditawarkan)
            component = row.get('Description - Item yang Ditawarkan', '')
            
            # Add prefix "V - " if not already present
            if not pd.isna(component) and not str(component).startswith("V - "):
                component = f"V - {component}"
            
            # Dapatkan quantity
            quantity = row.get('Qty.', 0)
            uom_value = row.get('Unit of Measure', '')
            # Buat baris baru
            new_row = {
                'Sequence': sequence,
                'Product': product,
                'Product Variant': product,  # Product dan Product Variant sama
                'Quantity': 1.0,
                'BoM Type': 'Kit',
                'Company': company,
                'Unit of Measure': uom_value,
                'BoM Lines/Component': component,
                'BoM Lines/Quantity': quantity
            }
            
            rows.append(new_row)
        
        # Buat DataFrame baru dari rows
        if rows:
            df_bom = pd.DataFrame(rows)
            
            try:
                # Pastikan data terurut dengan benar sebelum menerapkan DuplicateSuppressor
                df_bom = df_bom.sort_values(by=['Product', 'Product Variant']).reset_index(drop=True)
                
                # Terapkan DuplicateSuppressor secara manual untuk kontrol lebih besar
                prev_product = None
                prev_variant = None
                
                for idx in range(len(df_bom)):
                    current_product = df_bom.at[idx, 'Product']
                    current_variant = df_bom.at[idx, 'Product Variant']
                    
                    # Jika product dan variant sama dengan baris sebelumnya, kosongkan nilai
                    if current_product == prev_product and current_variant == prev_variant:
                        for col in ["Sequence", "Product", "Product Variant", "Quantity", "BoM Type", "Company", "Unit of Measure"]:
                            df_bom.at[idx, col] = ""
                    else:
                        # Update nilai sebelumnya
                        prev_product = current_product
                        prev_variant = current_variant
            except Exception as e:
                print(f"Error applying manual DuplicateSuppressor: {str(e)}")
                # Jika error, coba gunakan DuplicateSuppressor asli
                try:
                    suppressor = DuplicateSuppressor(
                        columns_to_suppress=["Sequence", "Product", "Product Variant", "Quantity", "BoM Type", "Company", "Unit of Measure"],
                        sort_data=True
                    )
                    df_bom = suppressor.transform(df_bom)
                except Exception as e:
                    print(f"Error applying DuplicateSuppressor: {str(e)}")
                    # Jika masih error, lanjutkan dengan data asli
        
        # Simpan ke Excel jika OUTPUT_PATHS ada
        output_path = OUTPUT_PATHS.get('BillOfMaterial')
        if output_path:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            df_bom.to_excel(output_path, index=False)
        
        return df_bom
    
    except Exception as e:
        import traceback
        print(f"Error in df_BillOfMaterial.transform(): {str(e)}")
        print(traceback.format_exc())
        # Return empty DataFrame with correct columns
        return pd.DataFrame(columns=[
            'Sequence', 'Product', 'Product Variant', 'Quantity', 
            'BoM Type', 'Company', 'BoM Lines/Component', 'BoM Lines/Quantity'
        ])

if __name__ == "__main__":
    # Test the module
    df_bom = transform()
    print(df_bom.head())
