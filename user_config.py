import json
import os
import streamlit as st

# File untuk menyimpan konfigurasi user
CONFIG_DIR = "temp"
USER_CONFIG_FILE = os.path.join(CONFIG_DIR, "user_config.json")

def save_config(config_data):
    """
    Menyimpan konfigurasi pengguna ke file JSON
    
    Args:
        config_data (dict): Data konfigurasi yang akan disimpan
    """
    try:
        # Pastikan direktori ada
        os.makedirs(CONFIG_DIR, exist_ok=True)
        
        # Simpan konfigurasi ke file JSON
        with open(USER_CONFIG_FILE, 'w') as f:
            json.dump(config_data, f, indent=4)
            
        return True
    except Exception as e:
        print(f"Error saving config: {str(e)}")
        return False

def load_config():
    """
    Memuat konfigurasi pengguna dari file JSON
    
    Returns:
        dict: Data konfigurasi yang dimuat atau dictionary kosong jika file tidak ditemukan
    """
    try:
        if os.path.exists(USER_CONFIG_FILE):
            with open(USER_CONFIG_FILE, 'r') as f:
                user_config = json.load(f)
            return user_config
        else:
            return {}
    except Exception as e:
        print(f"Error loading config: {str(e)}")
        return {}

def get_config(key, default_value=None):
    """
    Mendapatkan nilai konfigurasi dari file atau default jika tidak ditemukan
    
    Args:
        key (str): Kunci konfigurasi yang dicari
        default_value: Nilai default jika kunci tidak ditemukan
        
    Returns:
        Nilai konfigurasi atau default jika tidak ditemukan
    """
    config = load_config()
    return config.get(key, default_value)

def initialize_config():
    """
    Memuat konfigurasi pada startup dan menyimpannya ke session state
    """
    config = load_config()
    
    # Set session state variables from config
    # Ini hanya akan mengatur nilai jika belum ada di session_state
    if 'show_df_base' not in st.session_state:
        st.session_state.show_df_base = config.get('show_df_base', False)
    
    # Untuk konfigurasi Odoo
    if 'odoo_config' not in st.session_state:
        st.session_state.odoo_config = config.get('odoo', {})
