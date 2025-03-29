# config.py
# Konfigurasi default dan konstanta untuk aplikasi Excel Converter
import json
import os

# File untuk menyimpan konfigurasi user
USER_CONFIG_FILE = "temp/user_config.json"


# Pengaturan default untuk aplikasi
DEFAULT_SETTINGS = {
    "company": "PT. Visiniaga Mitra Kreasindo",
    "companies": ["PT. Visiniaga Mitra Kreasindo", "CV. Kreasi Andalan Karya"],
    "so_number": "",  # Default empty string
    "boq": {
        "header_row": 12,
        "data_start_row": 13,
        "data_end_row": 120,
    },
    "so": {
        "header_row": 1,
        "data_start_row": 2,
        "data_end_row": None,
    },
    "show_df_base": False,
}
# Odoo connection configuration
ODOO_CONFIG = {
    'url': "https://api-odoo.visiniaga.com",  # Replace with your Odoo URL
    'db': 'Final6.Copy250301',                    # Replace with your database name
    'username': "odoo2@visiniaga.com",              # Replace with your username
    'password': "PH8EQ?YF}<ac2A:T9n6%^*",              # Replace with your password
    'enabled': True                          # Set to True when you want to enable Odoo connection
}
# Path untuk file temporary
TEMP_PATH = "temp/"
DF_BASE_PATH = TEMP_PATH + "df_base.pkl"

# Output file paths
OUTPUT_PATHS = {
    "ProductVariant": TEMP_PATH + "output_products.xlsx",
    "BillOfMaterial": TEMP_PATH + "output_bom.xlsx",
    "SalesOrder": TEMP_PATH + "output_so.xlsx",
    "UpdateProduct": TEMP_PATH + "output_update.xlsx"
}

# Nama kolom untuk validasi
BOQ_VALIDATION_COL = "Description - Item yang Ditawarkan"
SO_VALIDATION_COL = "BOM Line"

# Fungsi untuk menyimpan konfigurasi ke file
def save_user_config(config_data):
    """
    Menyimpan konfigurasi pengguna ke file
    
    Args:
        config_data (dict): Data konfigurasi yang akan disimpan
    """
    # Pastikan folder temp ada
    os.makedirs(TEMP_PATH, exist_ok=True)
    
    # Simpan konfigurasi ke file JSON
    with open(USER_CONFIG_FILE, 'w') as f:
        json.dump(config_data, f, indent=4)

# Fungsi untuk memuat konfigurasi dari file
def load_user_config():
    """
    Memuat konfigurasi pengguna dari file
    
    Returns:
        dict: Data konfigurasi yang dimuat atau default jika file tidak ditemukan
    """
    try:
        if os.path.exists(USER_CONFIG_FILE):
            with open(USER_CONFIG_FILE, 'r') as f:
                user_config = json.load(f)
            return user_config
        else:
            return {}
    except Exception as e:
        print(f"Error loading user config: {str(e)}")
        return {}

# Muat konfigurasi pengguna dan gabungkan dengan default
USER_SETTINGS = {**DEFAULT_SETTINGS}
user_config = load_user_config()
if user_config:
    # Update default settings dengan nilai dari user config
    for key, value in user_config.items():
        if key in USER_SETTINGS:
            USER_SETTINGS[key] = value