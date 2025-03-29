import streamlit as st
from streamlit.components.v1 import html
import pandas as pd
import numpy as np
import os
import pickle
from pathlib import Path
import io
import re
import user_config


# Import library ETL
import sys
sys.path.append(".")
from ETL_library.extract import create_extractor
from ETL_library.transform import WhitespaceCleaner
from ETL_library.validate import CrossFileValidator
from df_transformation import df_UpdateProduct
from df_transformation import df_validation


# Import konfigurasi
from config import DEFAULT_SETTINGS, USER_SETTINGS, TEMP_PATH, DF_BASE_PATH, BOQ_VALIDATION_COL, SO_VALIDATION_COL, OUTPUT_PATHS, save_user_config, load_user_config

# Buat folder temp jika belum ada
Path(TEMP_PATH).mkdir(parents=True, exist_ok=True)

# Fungsi untuk menyimpan state aplikasi
def initialize_session_state():
    """Inisialisasi session state jika belum ada"""
    # Load user config pada startup
    user_config.initialize_config()
    if 'current_step' not in st.session_state:
        st.session_state.current_step = "upload"
    if 'boq_df' not in st.session_state:
        st.session_state.boq_df = None
    if 'so_df' not in st.session_state:
        st.session_state.so_df = None
    if 'validation_result' not in st.session_state:
        st.session_state.validation_result = None
    if 'validation_passed' not in st.session_state:
        st.session_state.validation_passed = False
    if 'df_base_processed' not in st.session_state:
        st.session_state.df_base_processed = False
    if 'boq_file' not in st.session_state:
        st.session_state.boq_file = None
    if 'so_file' not in st.session_state:
        st.session_state.so_file = None
    if 'boq_sheets' not in st.session_state:
        st.session_state.boq_sheets = []
    if 'so_sheets' not in st.session_state:
        st.session_state.so_sheets = []
    if 'boq_settings' not in st.session_state:
        st.session_state.boq_settings = {
            "sheet_name": None,
            "header_row": DEFAULT_SETTINGS["boq"]["header_row"],
            "data_start_row": DEFAULT_SETTINGS["boq"]["data_start_row"],
            "data_end_row": DEFAULT_SETTINGS["boq"]["data_end_row"]
        }
    if 'so_settings' not in st.session_state:
        st.session_state.so_settings = {
            "sheet_name": None,
            "header_row": DEFAULT_SETTINGS["so"]["header_row"],
            "data_start_row": DEFAULT_SETTINGS["so"]["data_start_row"],
            "data_end_row": DEFAULT_SETTINGS["so"]["data_end_row"]
        }
    if 'company' not in st.session_state:
        st.session_state.company = DEFAULT_SETTINGS["company"]
    # Tambahkan state untuk masing-masing output
    for output_type in ["ProductVariant", "BillOfMaterial", "SalesOrder", "UpdateProduct"]:
        if f'df_{output_type}' not in st.session_state:
            st.session_state[f'df_{output_type}'] = None
    
    if 'active_tab' not in st.session_state:
        st.session_state.active_tab = "Upload"  # Tab default
    if 'next_tab' not in st.session_state:
        st.session_state.next_tab = None
    if 'show_upload_message' not in st.session_state:
        st.session_state.show_upload_message = False
    if 'show_preview_message' not in st.session_state:
        st.session_state.show_preview_message = False
    if 'so_number' not in st.session_state:
        st.session_state.so_number = DEFAULT_SETTINGS.get("so_number", "")

def on_tab_change():
    # Fungsi ini akan dipanggil saat kita ingin mengubah tab
    # Ini mengatasi masalah dengan session_state
    pass

# Set page config
st.set_page_config(
    page_title="Excel Converter",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)
# Inisialisasi session state
initialize_session_state()

# Helper functions
def get_sheet_names(uploaded_file):
    """Ekstrak nama sheet dari file Excel yang diupload"""
    if uploaded_file is None:
        return []
    
    # Simpan file sementara
    temp_path = f"{TEMP_PATH}temp_file.xlsx"
    with open(temp_path, "wb") as f:
        f.write(uploaded_file.getvalue())
    
    # Baca sheet names
    import openpyxl
    wb = openpyxl.load_workbook(temp_path, read_only=True)
    sheet_names = wb.sheetnames
    wb.close()
    
    # Hapus file sementara
    os.remove(temp_path)
    
    return sheet_names

def extract_data(file, settings):
    """Extract data from file using settings"""
    if file is None:
        return None
        
    # Simpan file sementara untuk ekstraksi
    temp_path = f"{TEMP_PATH}temp_extract.xlsx"
    with open(temp_path, "wb") as f:
        f.write(file.getvalue())
    
    try:
        # Tentukan apakah file adalah xlsx atau csv
        file_ext = file.name.split(".")[-1].lower()
        
        # Adjust header_row and data_start_row for Excel (1-based indexing)
        header_row = settings["header_row"]
        data_start_row = settings["data_start_row"]
        data_end_row = settings["data_end_row"]
        
        # Buat extractor
        extractor = create_extractor(
            file_path=temp_path,
            sheet_name=settings["sheet_name"],
            header_row=header_row,
            data_start_row=data_start_row,
            data_end_row=data_end_row
        )
        
        # Extract data
        if file_ext in ["xlsx", "xls", "xlsm"]:
            df, _ = extractor.extract()
        else:
            df = extractor.extract()  # CSV extractor doesn't return border info
        
        # Bersihkan whitespace
        cleaner = WhitespaceCleaner(threshold=1)
        df = cleaner.clean(df)
        
        return df
    except Exception as e:
        st.error(f"Error extracting data: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return None
    finally:
        # Hapus file sementara
        if os.path.exists(temp_path):
            os.remove(temp_path)

def validate_files(boq_df, so_df):
    """Validate data between BoQ and Convert to SO files using new validation logic"""
    if boq_df is None or so_df is None:
        return False, None
    
    # Check if validation columns exist
    if BOQ_VALIDATION_COL not in boq_df.columns:
        st.error(f"Column '{BOQ_VALIDATION_COL}' not found in BoQ file. Available columns: {list(boq_df.columns)}")
        return False, None
    
    if SO_VALIDATION_COL not in so_df.columns:
        st.error(f"Column '{SO_VALIDATION_COL}' not found in Convert to SO file. Available columns: {list(so_df.columns)}")
        return False, None
    
    try:
        # Use the new validation module
        validation_results = df_validation.validate(boq_df, so_df, BOQ_VALIDATION_COL, SO_VALIDATION_COL)
        
        # Check if there are potential mismatches
        is_match = len(validation_results["potential_mismatch"]) == 0
        
        # Update the dataframes with the processed versions
        if "boq_df" in validation_results and "so_df" in validation_results:
            st.session_state.boq_df = validation_results["boq_df"]
            st.session_state.so_df = validation_results["so_df"]
        
        return is_match, validation_results
    except Exception as e:
        import traceback
        st.error(f"Error during validation: {str(e)}")
        st.error(traceback.format_exc())
        # Return None as fallback for validation_results
        return False, None

def process_df_base():
    """Process and save base dataframes to file"""
    try:
        # Import df_base module dynamically once it's implemented
        try:
            from df_transformation import df_base
            boq_df, so_df = df_base.transform_and_save(st.session_state.boq_df, st.session_state.so_df)
            st.session_state.boq_df = boq_df
            st.session_state.so_df = so_df
            st.session_state.df_base_processed = True
        except ImportError:
            # If module not implemented yet, just save the raw dataframes
            with open(DF_BASE_PATH, 'wb') as f:
                pickle.dump({
                    'boq_df': st.session_state.boq_df,
                    'so_df': st.session_state.so_df
                }, f)
            st.session_state.df_base_processed = True
    except Exception as e:
        st.error(f"Error processing base dataframes: {str(e)}")
        st.session_state.df_base_processed = False

def download_excel(df, filename):
    """Create download button for Excel file"""
    if df is not None:
        # Create Excel file in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1')
        
        # Offer download
        st.download_button(
            label="Download Excel",
            data=output.getvalue(),
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            help="Download the edited data as Excel file"
        )
    else:
        st.warning("No data available to download.")

def load_df_base():
    """Load base dataframes from pickle file"""
    try:
        if os.path.exists(DF_BASE_PATH):
            with open(DF_BASE_PATH, 'rb') as f:
                data = pickle.load(f)
                return data['boq_df'], data['so_df']
        return None, None
    except Exception as e:
        st.error(f"Error loading base dataframes: {str(e)}")
        return None, None

def process_transformation(transform_type):
    """Process transformation for specified type"""
    try:
        module_name = f"df_{transform_type}"
        # Try to import the module dynamically
        try:
            module = __import__(f"df_transformation.{module_name}", fromlist=[None])
            df = module.transform()
            st.session_state[f'df_{transform_type}'] = df
            return df
        except ImportError:
            st.info(f"Module df_transformation.{module_name} not implemented yet.")
            # Return empty DataFrame as placeholder
            return pd.DataFrame(columns=[f"{transform_type} will be implemented"])
    except Exception as e:
        st.error(f"Error processing {transform_type} transformation: {str(e)}")
        return None

# Title and main tabs
st.title("Excel Converter")

# Cek apakah ada permintaan perpindahan tab
if st.session_state.next_tab is not None:
    # Gunakan nilai ini untuk tab aktif
    pre_selected = st.session_state.next_tab
    # Reset untuk penggunaan berikutnya
    st.session_state.next_tab = None
else:
    # Gunakan nilai yang sudah ada jika tidak ada permintaan perpindahan
    pre_selected = st.session_state.active_tab if 'active_tab' in st.session_state else "Upload"

# Radio button untuk navigasi tab
st.sidebar.radio(
    "Navigation", 
    ["Configuration", "Upload", "Preview", "Transform"],
    key="active_tab",
    index=["Configuration", "Upload", "Preview", "Transform"].index(pre_selected),
    label_visibility="hidden"  # Sembunyikan label
)

tabs = ["Configuration", "Upload", "Preview", "Transform"]
tab_config, tab_upload, tab_preview, tab_transform = st.tabs(tabs)


# Tab 0 : Odoo Configuration
with tab_config:
    st.header("Odoo API Configuration")
    
    # Import Odoo config dari file config.py
    from config import ODOO_CONFIG
    
    # Form untuk konfigurasi Odoo
    with st.form("odoo_config_form"):
        st.subheader("Odoo Connection Settings")
        
        # URL
        odoo_url = st.text_input(
            "Odoo URL", 
            value=st.session_state.odoo_config.get('url', ODOO_CONFIG.get('url', "https://")),
            help="URL lengkap server Odoo, contoh: https://api-odoo.example.com"
        )
        
        # Database
        odoo_db = st.text_input(
            "Database Name", 
            value=st.session_state.odoo_config.get('db', ODOO_CONFIG.get('db', "")),
            help="Nama database Odoo"
        )
        
        # Username
        odoo_username = st.text_input(
            "Username", 
            value=st.session_state.odoo_config.get('username', ODOO_CONFIG.get('username', "")),
            help="Username/email untuk login ke Odoo"
        )
        
        # Password
        odoo_password = st.text_input(
            "Password", 
            value=st.session_state.odoo_config.get('password', ODOO_CONFIG.get('password', "")),
            type="password",
            help="Password untuk login ke Odoo"
        )
        
        # Enable/Disable Odoo connection
        odoo_enabled = st.checkbox(
            "Enable Odoo Connection", 
            value=st.session_state.odoo_config.get('enabled', ODOO_CONFIG.get('enabled', False)),
            help="Aktifkan/Nonaktifkan koneksi ke Odoo"
        )
        
        # Toggle untuk df-base visibility
        st.divider()
        st.subheader("Display Settings")
        show_df_base = st.checkbox(
            "Show Raw data Tab", 
            value=st.session_state.show_df_base,
            help="Aktifkan/Nonaktifkan tampilan tab df-base"
        )
        
        # Submit button
        submitted = st.form_submit_button("Save Configuration")

        # Proses ketika form di-submit
        if submitted:
            # Simpan ke session state
            st.session_state.show_df_base = show_df_base
            
            # Simpan konfigurasi Odoo ke session state
            st.session_state.odoo_config = {
                'url': odoo_url,
                'db': odoo_db,
                'username': odoo_username,
                'password': odoo_password,
                'enabled': odoo_enabled
            }
            
            # Simpan konfigurasi ke file
            config_to_save = {
                'show_df_base': show_df_base,
                'odoo': {
                    'url': odoo_url,
                    'db': odoo_db,
                    'username': odoo_username,
                    'password': odoo_password,
                    'enabled': odoo_enabled
                }
            }
            
            success = user_config.save_config(config_to_save)
            
            # Tampilkan notifikasi
            if success:
                st.success("Configuration saved successfully! Settings will persist when you reload the app.")

                # Reset semua hasil transformasi ketika konfigurasi berubah
                for output_type in ["ProductVariant", "BillOfMaterial", "SalesOrder", "UpdateProduct"]:
                    if f'df_{output_type}' in st.session_state:
                        st.session_state[f'df_{output_type}'] = None
                
                # Reset data vendor sama juga
                if 'df_UpdateProduct_same_vendor' in st.session_state:
                    st.session_state.df_UpdateProduct_same_vendor = None
                    
                # Tampilkan pesan tambahan
                st.info("Transformation results have been reset. Please re-process your data to use the new configuration.")
            else:
                st.error("Failed to save configuration. Please try again or check folder permissions.")
            
            # Dalam implementasi nyata, Anda bisa menyimpan konfigurasi ke file
            # Contoh pseudocode:
            # save_config_to_file({
            #     'url': odoo_url,
            #     'db': odoo_db,
            #     'username': odoo_username,
            #     'password': odoo_password,
            #     'enabled': odoo_enabled
            # })


# Tab 1: Upload & Configure
with tab_upload:
    if st.session_state.current_step == "upload":
        st.header("Upload & Configure Files")
        
        # Two-column layout for file uploaders
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Bill of Quantity (BoQ)")
            boq_file = st.file_uploader("Upload BoQ File", type=["xlsx", "xls"], key="boq_uploader")
            
            # Process BoQ file
            if boq_file is not None and (st.session_state.boq_file is None or boq_file.name != st.session_state.boq_file.name):
                st.session_state.boq_file = boq_file
                st.session_state.boq_sheets = get_sheet_names(boq_file)
                if st.session_state.boq_sheets:
                    st.session_state.boq_settings["sheet_name"] = st.session_state.boq_sheets[0]
            
            # BoQ settings
            if st.session_state.boq_file is not None:
                with st.expander("BoQ Settings", expanded=True):
                    # Collect inputs without strict validation
                    sheet_name = st.selectbox(
                        "Select Sheet", 
                        st.session_state.boq_sheets,
                        index=0 if st.session_state.boq_settings["sheet_name"] is None else 
                              st.session_state.boq_sheets.index(st.session_state.boq_settings["sheet_name"]),
                        key="boq_sheet"
                    )
                    
                    header_row = st.number_input(
                        "Header Row", 
                        min_value=0, 
                        value=st.session_state.boq_settings["header_row"],
                        help="Row number containing column headers (0-based)",
                        key="boq_header"
                    )
                    
                    # Set minimum sensible value but don't enforce header_row + 1
                    data_start_row = st.number_input(
                        "Data Start Row", 
                        min_value=0, 
                        value=max(st.session_state.boq_settings["data_start_row"], header_row + 1),
                        help="Row number where data begins (0-based). Must be greater than Header Row.",
                        key="boq_data_start"
                    )
                    
                    data_end_row = st.number_input(
                        "Data End Row ", 
                        min_value=0, 
                        value=1000 if st.session_state.boq_settings["data_end_row"] is None else st.session_state.boq_settings["data_end_row"],
                        help="Row number where data ends",
                        key="boq_data_end"
                    )
                    
                    # Update settings
                    st.session_state.boq_settings = {
                        "sheet_name": sheet_name,
                        "header_row": int(header_row),
                        "data_start_row": int(data_start_row),
                        "data_end_row": int(data_end_row) if data_end_row > 0 else None
                    }
        
        with col2:
            st.subheader("Convert to SO")
            so_file = st.file_uploader("Upload Convert to SO File", type=["xlsx", "xls"], key="so_uploader")
            
            # Process SO file
            if so_file is not None and (st.session_state.so_file is None or so_file.name != st.session_state.so_file.name):
                st.session_state.so_file = so_file
                st.session_state.so_sheets = get_sheet_names(so_file)
                if st.session_state.so_sheets:
                    st.session_state.so_settings["sheet_name"] = st.session_state.so_sheets[0]
            
            # SO settings
            if st.session_state.so_file is not None:
                with st.expander("Convert to SO Settings", expanded=True):
                    # Collect inputs without strict validation
                    sheet_name = st.selectbox(
                        "Select Sheet", 
                        st.session_state.so_sheets,
                        index=0 if st.session_state.so_settings["sheet_name"] is None else 
                              st.session_state.so_sheets.index(st.session_state.so_settings["sheet_name"]),
                        key="so_sheet"
                    )
                    
                    header_row = st.number_input(
                        "Header Row", 
                        min_value=0, 
                        value=st.session_state.so_settings["header_row"],
                        help="Row number containing column headers (0-based)",
                        key="so_header"
                    )
                    
                    # Set minimum sensible value but don't enforce header_row + 1
                    data_start_row = st.number_input(
                        "Data Start Row", 
                        min_value=0, 
                        value=max(st.session_state.so_settings["data_start_row"], header_row + 1),
                        help="Row number where data begins (0-based). Must be greater than Header Row.",
                        key="so_data_start"
                    )
                    
                    data_end_row = st.number_input(
                        "Data End Row (Optional)", 
                        min_value=0, 
                        value=1000 if st.session_state.so_settings["data_end_row"] is None else st.session_state.so_settings["data_end_row"],
                        help="Row number where data ends (optional)",
                        key="so_data_end"
                    )
                    
                    # Update settings
                    st.session_state.so_settings = {
                        "sheet_name": sheet_name,
                        "header_row": int(header_row),
                        "data_start_row": int(data_start_row),
                        "data_end_row": int(data_end_row) if data_end_row > 0 else None
                    }
        
        # Company selection
        st.subheader("Company Selection")
        st.session_state.company = st.selectbox(
            "Select Company",
            DEFAULT_SETTINGS["companies"],
            index=DEFAULT_SETTINGS["companies"].index(st.session_state.company),
            key="company_select"
        )
        # SO Number input
        st.subheader("SO Number")
        st.session_state.so_number = st.text_input(
            "Enter SO Number", 
            value=st.session_state.so_number,
            placeholder="e.g., 250300113",
            help="Enter the SO number for processing"
        )
        
        # Validate files button
        col1, col2, col3 = st.columns([2, 1, 1])
        with col3:
            if st.button("Validate Files", type="primary", key="validate_button", use_container_width=True):
                if st.session_state.boq_file is None or st.session_state.so_file is None:
                    st.error("Please upload both BoQ and Convert to SO files.")
                
                # Validate input settings
                elif (st.session_state.boq_settings["header_row"] >= st.session_state.boq_settings["data_start_row"] or
                      st.session_state.so_settings["header_row"] >= st.session_state.so_settings["data_start_row"]):
                    
                    error_message = ""
                    if st.session_state.boq_settings["header_row"] >= st.session_state.boq_settings["data_start_row"]:
                        error_message += "BoQ: Header Row must be less than Data Start Row. "
                    
                    if st.session_state.so_settings["header_row"] >= st.session_state.so_settings["data_start_row"]:
                        error_message += "Convert to SO: Header Row must be less than Data Start Row."
                    
                    st.error(error_message)
                
                else:
                    with st.spinner("Extracting and validating data..."):
                        # Extract data
                        st.session_state.boq_df = extract_data(st.session_state.boq_file, st.session_state.boq_settings)
                        st.session_state.so_df = extract_data(st.session_state.so_file, st.session_state.so_settings)
                        
                        if st.session_state.boq_df is not None and st.session_state.so_df is not None:
                            # Validate files
                            is_match, validation_report = validate_files(st.session_state.boq_df, st.session_state.so_df)
                            st.session_state.validation_passed = is_match
                            st.session_state.validation_result = validation_report
                            st.session_state.current_step = "preview"

                            # Set active tab to "Preview"
                            st.session_state.show_upload_message = True
                            st.session_state.next_tab = "Preview"
                            st.rerun()
                        else:
                            st.error("Error extracting data from files. Please check your settings.")
        # Tambahkan TEPAT SETELAH baris st.header("Upload & Configure Files")
        if st.session_state.show_upload_message:
            st.success("✅ Data validation complete! Silahkan lanjut ke tab **Preview**")
            if st.button("Go to Preview Tab", key="goto_preview"):
                st.session_state.next_tab = "Preview"
                st.rerun()         

# Tab 2: Preview & Validate
with tab_preview:
    if st.session_state.current_step == "preview":
        st.header("Preview & Validation")

        # Display validation results
        if st.session_state.validation_result is not None:
            st.header("Validation Results")
            
            validation_results = st.session_state.validation_result
            
            # Tampilkan ringkasan validasi
            

            items_count = len(validation_results.get("items", []))
            single_products_count = len(validation_results.get("single_product", []))
            potential_mismatch_count = len(validation_results.get("potential_mismatch", []))
            
            is_dark_theme = st.get_option("theme.base") == "dark"
            status_color = "green" if potential_mismatch_count == 0 else "orange"
            bg_color = "#000000" if is_dark_theme else "#f0f2f6"
            text_color = "#FFFFFF" if is_dark_theme else "#000000"
            
            # Deteksi jenis status untuk menentukan jenis pesan
            if potential_mismatch_count == 0:
                st.success(f"""
                ### Validasi data selesai
                - Item yang ditemukan: {items_count} Item
                - Produk tunggal ditemukan: {single_products_count} Produk
                - Potensi ketidakcocokan data: {potential_mismatch_count} item
                """)
            else:
                st.warning(f"""
                ### Validasi data selesai
                - Item yang ditemukan: {items_count} Item
                - Produk tunggal ditemukan: {single_products_count} Produk
                - Potensi ketidakcocokan data: {potential_mismatch_count} item
                """)
            
            # Create tabs for different validation results
            validation_tabs = st.tabs(["Potential Mismatches", "Items", "Single Products"])
            
            # Tab 1: Potential Mismatches
            with validation_tabs[0]:
                st.subheader("Potential Mismatches")
                if potential_mismatch_count > 0:
                    # Tambahkan warna background merah untuk tab ini
                    st.markdown(
                        """
                        <style>
                        div[data-testid="stHorizontalBlock"] div[role="tab"]:first-child {
                            background-color: rgba(255, 0, 0, 0.2);
                            border-radius: 4px;
                        }
                        </style>
                        """, 
                        unsafe_allow_html=True
                    )
                    # Buat kolom 'type' jika belum ada
                    potential_mismatch_df = pd.DataFrame(validation_results["potential_mismatch"])
                    if 'type' not in potential_mismatch_df.columns:
                        potential_mismatch_df['type'] = 'Data Mismatch'
                        
                    st.dataframe(potential_mismatch_df, use_container_width=True)
                    
                    st.markdown("""
                    **Keterangan:**
                    - **Data Mismatch**: Data yang tidak ditemukan kecocokannya antara BoQ dan Convert to SO
                    - **UoM Anomaly**: Anomali terkait Unit of Measure (UoM tidak lengkap atau tidak sesuai)
                    - Anda dapat melanjutkan proses data, tetapi perhatikan bahwa item-item ini mungkin memerlukan pengolahan tambahan
                    """)
                else:
                    st.success("Tidak ada ketidakcocokan yang ditemukan. Semua data sudah sesuai.")

            # Tab 2: Items
            with validation_tabs[1]:
                st.subheader("Items (Kategori)")
                if items_count > 0:
                    items_df = pd.DataFrame(validation_results["items"])
                    st.dataframe(items_df, use_container_width=True)
                else:
                    st.info("Tidak ada data yang teridentifikasi sebagai Items.")
            
            # Tab 3: Single Products
            with validation_tabs[2]:
                st.subheader("Single Products")
                if single_products_count > 0:
                    single_products_df = pd.DataFrame(validation_results["single_product"])
                    st.dataframe(single_products_df, use_container_width=True)
                else:
                    st.info("Tidak ada data yang teridentifikasi sebagai Single Products.")
            
            


        # Display data previews
        st.subheader("Data Preview")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("BoQ Data Preview")
            if st.session_state.boq_df is not None:
                st.dataframe(st.session_state.boq_df.head(10), use_container_width=True)
            else:
                st.info("No BoQ data available.")
        
        with col2:
            st.write("Convert to SO Data Preview")
            if st.session_state.so_df is not None:
                st.dataframe(st.session_state.so_df.head(10), use_container_width=True)
            else:
                st.info("No Convert to SO data available.")
        
        # Navigation buttons
        col1, col2, col3 = st.columns([1, 2, 1])
        
        with col1:
            if st.button("← Back to Upload", key="back_to_upload"):
                st.session_state.current_step = "upload"
                st.rerun()
        
        with col3:
            if st.button("Process Data →", type="primary", key="process_data", use_container_width=True):
                if st.session_state.boq_df is not None and st.session_state.so_df is not None:
                    with st.spinner("Processing data..."):
                        process_df_base()
                        st.session_state.current_step = "transform"

                        # Set active tab to "Transform" 
                        st.session_state.show_preview_message = True
                        st.session_state.next_tab = "Transform"
                        st.rerun()
                else:
                    st.error("No data available to process.")
        # Tambahkan TEPAT SETELAH baris st.header("Preview & Validation") 
        if st.session_state.show_preview_message:
            st.success("✅ Data processing complete! Silahkan lanjut ke tab **Transform**")
            if st.button("Go to Transform Tab", key="goto_transform"):
                st.session_state.next_tab = "Transform"
                st.rerun()
                
# Tab 3: Transform
with tab_transform:
    if st.session_state.current_step == "transform":
        st.header("Data Transformation")


        # Create sub-tabs for different transformations
        # Tentukan tab yang akan ditampilkan berdasarkan toggle
        if st.session_state.show_df_base:
            transform_tab_names = [
                "Raw Data", 
                "df-ProductVariant",
                "df-UpdateProduct", 
                "df-BillOfMaterial", 
                "df-SalesOrder", 
                
            ]
        else:
            transform_tab_names = [
                "df-ProductVariant",
                "df-UpdateProduct", 
                "df-BillOfMaterial", 
                "df-SalesOrder", 
                
            ]

        # Buat tab berdasarkan daftar nama tab
        transform_tabs = st.tabs(transform_tab_names)

        # Load base data if not in session state
        if not st.session_state.df_base_processed:
            st.session_state.boq_df, st.session_state.so_df = load_df_base()
            if st.session_state.boq_df is not None and st.session_state.so_df is not None:
                st.session_state.df_base_processed = True

        # Inisialisasi indeks tab
        current_tab = 0

        # Tab: df-base (hanya jika toggle aktif)
        if st.session_state.show_df_base:
            with transform_tabs[current_tab]:  # df-base tab
                st.subheader("Base DataFrames")
                
                # Display BoQ data
                with st.expander("BoQ Data", expanded=True):
                    if st.session_state.boq_df is not None:
                        # Menggunakan data_editor untuk memungkinkan pengeditan
                        edited_boq_df = st.data_editor(
                            st.session_state.boq_df,
                            use_container_width=True,
                            num_rows="dynamic", # Memungkinkan menambah baris baru
                            key="edit_boq_data"
                        )
                        # Simpan data yang diedit ke session state
                        st.session_state.boq_df = edited_boq_df
                    else:
                        st.info("No BoQ data available.")
                
                # Display Convert to SO data
                with st.expander("Convert to SO Data", expanded=True):
                    if st.session_state.so_df is not None:
                        # Menggunakan data_editor untuk memungkinkan pengeditan
                        edited_so_df = st.data_editor(
                            st.session_state.so_df,
                            use_container_width=True,
                            num_rows="dynamic", # Memungkinkan menambah baris baru
                            key="edit_so_data"
                        )
                        # Simpan data yang diedit ke session state
                        st.session_state.so_df = edited_so_df
                    else:
                        st.info("No Convert to SO data available.")
                
                # Download buttons
                col1, col2 = st.columns(2)
                with col1:
                    download_excel(st.session_state.boq_df, "boq_data.xlsx")
                with col2:
                    download_excel(st.session_state.so_df, "so_data.xlsx")
            
            # Pindah ke tab berikutnya
            current_tab += 1

        # Tab: df-ProductVariant
        with transform_tabs[current_tab]:  # ProductVariant tab
            st.subheader("Product Variant")
            
            # Process transformation if not already done
            if st.session_state.df_ProductVariant is None:
                with st.spinner("Processing Product Variant transformation..."):
                    df_product = process_transformation("ProductVariant")
            else:
                df_product = st.session_state.df_ProductVariant
            
            # Display data
            with st.expander("Product Variant Data", expanded=True):
                if df_product is not None and not df_product.empty:
                    # Menggunakan data_editor untuk memungkinkan pengeditan
                    edited_product_df = st.data_editor(
                        df_product,
                        use_container_width=True,
                        num_rows="dynamic", # Memungkinkan menambah baris baru
                        key="edit_product_data"
                    )
                    # Simpan data yang diedit ke session state
                    st.session_state.df_ProductVariant = edited_product_df
                else:
                    st.info("No Product Variant data available.")
            
            # Download and upload buttons
            col1, col2 = st.columns(2)
            with col1:
                download_excel(df_product, "product_variant.xlsx")
            with col2:
                st.button("Upload to Odoo", disabled=True, help="This feature will be enabled in a future update", key="upload_product_odoo")

        # Pindah ke tab berikutnya
        current_tab += 1

        
        # Tab: df-UpdateProduct
        with transform_tabs[current_tab]:  # UpdateProduct tab
            st.subheader("Update Product")
            
            # Process transformation if not already done
            if st.session_state.df_UpdateProduct is None:
                with st.spinner("Processing UpdateProduct transformation..."):
                    df_update = process_transformation("UpdateProduct")
            else:
                df_update = st.session_state.df_UpdateProduct
            
            # Ensure df_UpdateProduct_same_vendor exists in session state
            if 'df_UpdateProduct_same_vendor' not in st.session_state:
                st.session_state.df_UpdateProduct_same_vendor = pd.DataFrame(
                    columns=['id', 'default_code', 'name', 'seller_ids/id', 'seller_ids/price']
                )
            
            # Tabel Pertama: update info product dengan vendor beda
            st.markdown("### Update info product dengan vendor beda")
            with st.expander("Update Product Data", expanded=True):
                if df_update is not None and not df_update.empty:
                    # Menggunakan data_editor untuk memungkinkan pengeditan
                    edited_update_df = st.data_editor(
                        df_update,
                        use_container_width=True,
                        num_rows="dynamic", # Memungkinkan menambah baris baru
                        key="edit_update_product_data"
                    )
                    # Simpan data yang diedit ke session state
                    st.session_state.df_UpdateProduct = edited_update_df
                else:
                    st.info("No Update Product data available.")
            
            # Download button untuk tabel pertama
            if df_update is not None and not df_update.empty:
                download_excel(df_update, "update_product.xlsx")
            
            st.markdown("---")
            
            # Tabel Kedua: update info product dengan vendor sama & harga beda
            st.markdown("### Update info product dengan vendor sama & harga beda")
            with st.expander("Update Product Same Vendor Data", expanded=True):
                if st.session_state.df_UpdateProduct_same_vendor is not None and not st.session_state.df_UpdateProduct_same_vendor.empty:
                    # Menggunakan data_editor untuk memungkinkan pengeditan
                    edited_same_vendor_df = st.data_editor(
                        st.session_state.df_UpdateProduct_same_vendor,
                        use_container_width=True,
                        num_rows="dynamic", # Memungkinkan menambah baris baru
                        key="edit_same_vendor_data"
                    )
                    # Simpan data yang diedit ke session state
                    st.session_state.df_UpdateProduct_same_vendor = edited_same_vendor_df
                else:
                    st.info("No data available for products with same vendor but different prices.")
            
            # Download button untuk tabel kedua
            if st.session_state.df_UpdateProduct_same_vendor is not None and not st.session_state.df_UpdateProduct_same_vendor.empty:
                # Define path for the second table Excel file
                second_table_filename = "update_product_same_vendor.xlsx"
                download_excel(st.session_state.df_UpdateProduct_same_vendor, second_table_filename)
            
            st.markdown("---")
            
            # Tampilkan warning untuk produk yang bermasalah
            st.markdown("### Daftar Produk Bermasalah")
            try:
                problematic_products = df_UpdateProduct.get_missing_xml_ids()
                
                if problematic_products:
                    # Hitung jumlah produk berdasarkan status
                    no_xml_count = sum(1 for p in problematic_products if p.get('status') == 'Tanpa XML ID')
                    empty_id_count = sum(1 for p in problematic_products if p.get('status') == 'ID Tidak Ditemukan')
                    
                    # Tampilkan warning dengan detail jumlah
                    warning_msg = ""
                    if no_xml_count > 0:
                        warning_msg += f"⚠️ Terdapat {no_xml_count} produk yang belum memiliki XML ID di Odoo. "
                    if empty_id_count > 0:
                        warning_msg += f"⚠️ Terdapat {empty_id_count} produk yang ID-nya tidak ditemukan di Odoo."
                    
                    st.warning(warning_msg)
                    
                    # Tampilkan detail produk yang bermasalah
                    with st.expander("Lihat daftar produk bermasalah", expanded=False):
                        st.markdown("#### Produk Bermasalah")
                        st.markdown("""
                        Produk-produk berikut memiliki masalah baik karena:
                        - **Tanpa XML ID**: Produk ada di Odoo tapi belum memiliki XML ID (sebaiknya lakukan ekspor produk)
                        - **ID Tidak Ditemukan**: Produk tidak ditemukan di database Odoo (perlu dibuat terlebih dahulu)
                        """)
                        
                        # Buat tabel untuk menampilkan daftar produk
                        missing_df = pd.DataFrame(problematic_products)
                        # Urutkan berdasarkan status
                        if not missing_df.empty and 'status' in missing_df.columns:
                            missing_df = missing_df.sort_values(by='status')
                        
                        st.dataframe(missing_df, use_container_width=True)
                        
                        # Tambahkan petunjuk yang lebih detail
                        st.markdown("""
                        **Catatan:** 
                        - Produk tanpa XML ID: Produk sudah ada di Odoo tetapi akan menggunakan ID database sebagai fallback
                        - Produk dengan ID tidak ditemukan: Produk ini tidak ada di Odoo dan perlu dibuat terlebih dahulu
                        
                        Disarankan untuk:
                        1. Buat terlebih dahulu produk yang belum ada di Odoo
                        2. Ekspor produk dari UI Odoo untuk membuat XML ID yang sesuai
                        """)
            except Exception as e:
                st.error(f"Error menampilkan daftar produk bermasalah: {str(e)}")

            # Upload to Odoo button (disabled for now)
            st.button("Upload to Odoo", disabled=True, help="This feature will be enabled in a future update", key="upload_update_odoo")
        
        
        # Pindah ke tab berikutnya
        current_tab += 1

        # Tab: df-BillOfMaterial
        with transform_tabs[current_tab]:  # BillOfMaterial tab
            st.subheader("Bill Of Material")
            
            # Process transformation if not already done
            if st.session_state.df_BillOfMaterial is None:
                with st.spinner("Processing BillOfMaterial transformation..."):
                    df_bom = process_transformation("BillOfMaterial")
            else:
                df_bom = st.session_state.df_BillOfMaterial
            
            # Display data
            with st.expander("Bill Of Material Data", expanded=True):
                if df_bom is not None and not df_bom.empty:
                    # Menggunakan data_editor untuk memungkinkan pengeditan
                    edited_bom_df = st.data_editor(
                        df_bom,
                        use_container_width=True,
                        num_rows="dynamic", # Memungkinkan menambah baris baru
                        key="edit_bom_data"
                    )
                    # Simpan data yang diedit ke session state
                    st.session_state.df_BillOfMaterial = edited_bom_df
                else:
                    st.info("No Bill Of Material data available.")
            
            # Download and upload buttons
            col1, col2 = st.columns(2)
            with col1:
                download_excel(df_bom, "bill_of_material.xlsx")
            with col2:
                st.button("Upload to Odoo", disabled=True, help="This feature will be enabled in a future update", key="upload_bom_odoo")

        # Pindah ke tab berikutnya
        current_tab += 1

        # Tab: df-SalesOrder
        with transform_tabs[current_tab]:  # SalesOrder tab
            st.subheader("Sales Order")
            
            # Process transformation if not already done
            if st.session_state.df_SalesOrder is None:
                with st.spinner("Processing SalesOrder transformation..."):
                    df_so = process_transformation("SalesOrder")
            else:
                df_so = st.session_state.df_SalesOrder
            
            # Display data
            with st.expander("Sales Order Data", expanded=True):
                if df_so is not None and not df_so.empty:
                    # Menggunakan data_editor untuk memungkinkan pengeditan
                    edited_so_df = st.data_editor(
                        df_so,
                        use_container_width=True,
                        num_rows="dynamic", # Memungkinkan menambah baris baru
                        key="edit_sales_order_data"
                    )
                    # Simpan data yang diedit ke session state
                    st.session_state.df_SalesOrder = edited_so_df
                else:
                    st.info("No Sales Order data available.")
            
            # Download and upload buttons
            col1, col2 = st.columns(2)
            with col1:
                download_excel(df_so, "sales_order.xlsx")
            with col2:
                st.button("Upload to Odoo", disabled=True, help="This feature will be enabled in a future update", key="upload_so_odoo")

        
        # Back button
        if st.button("← Back to Preview", key="back_to_preview"):
            st.session_state.current_step = "preview"
            st.rerun()
