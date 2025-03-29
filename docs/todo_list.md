# To-Do List Pengembangan Excel Converter

## 1. Pembuatan Struktur Folder dan File Dasar

```
excel_converter/
├── ETL_library/                # Kopi semua file library yang sudah ada
│   ├── extract.py              # Fungsi ekstraksi Excel/CSV
│   ├── transform.py            # Komponen transformasi data
│   ├── load.py                 # Komponen loading/export data
│   ├── validate.py             # Fungsi validasi data
│   └── utility.py              # Fungsi-fungsi pendukung
│
├── df_transformation/          # Logic untuk transformasi dari df-base ke output
│   ├── df_base.py              # Transformasi data dari input menjadi df-base
│   ├── df_ProductVariant.py    # Transformasi df-base ke format ProductVariant
│   ├── df_BillOfMaterial.py    # Transformasi df-base ke format BillOfMaterial
│   ├── df_SalesOrder.py        # Transformasi df-base ke format SalesOrder
│   └── df_UpdateProduct.py     # Transformasi df-base ke format UpdateProduct
│
├── temp/                       # Folder penyimpanan data sementara
│   └── .gitkeep                # Pastikan folder ini ada di git
│
├── main.py                     # File utama aplikasi Streamlit
├── config.py                   # Konfigurasi default dan konstanta
└── requirements.txt            # Daftar dependencies
```

## 2. File config.py

Buat file konfigurasi dengan konstanta default:

```python
# config.py
DEFAULT_SETTINGS = {
    "company": "PT. Visiniaga Mitra Kreasindo",
    "companies": ["PT. Visiniaga Mitra Kreasindo", "CV. Kreasi Andalan Karya"],
    "header_row": 5,
    "data_start_row": 6,
    "data_end_row": None,
}

# Path untuk file temporary
TEMP_PATH = "temp/"
DF_BASE_PATH = TEMP_PATH + "df_base.pkl"

# Nama kolom untuk validasi
BOQ_VALIDATION_COL = "Description - Item yang Ditawarkan"
SO_VALIDATION_COL = "BOM Line"
```

## 3. Implementasi main.py dengan Streamlit

### 3.1. Import dan Setup

```python
import streamlit as st
import pandas as pd
import os
import pickle
from pathlib import Path

# Import library ETL
import sys
sys.path.append(".")
from ETL_library.extract import create_extractor
from ETL_library.transform import WhitespaceCleaner
from ETL_library.validate import CrossFileValidator

# Import modul transformasi
from df_transformation import df_base
from df_transformation import df_ProductVariant
from df_transformation import df_BillOfMaterial
from df_transformation import df_SalesOrder
from df_transformation import df_UpdateProduct

# Import konfigurasi
from config import DEFAULT_SETTINGS, TEMP_PATH, DF_BASE_PATH

# Buat folder temp jika belum ada
Path(TEMP_PATH).mkdir(parents=True, exist_ok=True)
```

### 3.2. Implementasi 3 Tab Utama

```python
st.title("Excel Converter")

# Buat 3 tab utama
tab_upload, tab_preview, tab_transform = st.tabs(["Upload", "Preview", "Transform"])

# Simpan state aplikasi di session state
if 'current_step' not in st.session_state:
    st.session_state.current_step = "upload"
if 'boq_df' not in st.session_state:
    st.session_state.boq_df = None
if 'so_df' not in st.session_state:
    st.session_state.so_df = None
if 'validation_result' not in st.session_state:
    st.session_state.validation_result = None
if 'df_base_processed' not in st.session_state:
    st.session_state.df_base_processed = False
```

### 3.3. Tab Upload

Implementasi form upload dengan:
- File uploader untuk BoQ dan Convert to SO
- Dropdown untuk sheet selection
- Input fields untuk header_row, data_start_row, data_end_row
- Dropdown untuk company selection
- Tombol "Validate Files"

```python
with tab_upload:
    if st.session_state.current_step == "upload":
        # Form layout here
        # ...
```

### 3.4. Tab Preview & Validasi

Implementasikan tampilan preview dengan:
- Menampilkan hasil validasi (sukses/error)
- Jika error, tampilkan detail ketidakcocokan
- Preview data dari kedua file
- Tombol "Process Data" yang aktif meskipun ada error

```python
with tab_preview:
    if st.session_state.current_step == "preview":
        # Validation results and preview
        # ...
```

### 3.5. Tab Transform dengan 5 Sub-tab

Implementasikan 5 sub-tab dengan:
- df-base: menampilkan 2 tabel terpisah (BoQ dan Convert to SO)
- 4 sub-tab lainnya: menampilkan hasil transformasi dan tombol Download/Upload

```python
with tab_transform:
    if st.session_state.current_step == "transform":
        transform_tab = st.tabs(["df-base", "df-ProductVariant", "df-BillOfMaterial", "df-SalesOrder", "df-UpdateProduct"])
        
        with transform_tab[0]:  # df-base
            # Tampilkan 2 tabel dengan fitur expand/minimize
            # ...
            
        with transform_tab[1]:  # df-ProductVariant
            # Tampilkan hasil transformasi ProductVariant
            # ...
            
        # Implementasi tab lainnya
        # ...
```

## 4. Implementasi df_transformation/df_base.py

```python
# df_base.py
import pandas as pd
import pickle
from config import DF_BASE_PATH

def transform_and_save(boq_df, so_df):
    """
    Transform dan simpan data dari kedua file input
    
    Args:
        boq_df (pd.DataFrame): DataFrame dari file BoQ
        so_df (pd.DataFrame): DataFrame dari file Convert to SO
        
    Returns:
        tuple: (boq_df, so_df) setelah transformasi
    """
    # Implementasikan logic transformasi data
    # ...
    
    # Simpan kedua DataFrame ke pickle file
    with open(DF_BASE_PATH, 'wb') as f:
        pickle.dump({
            'boq_df': boq_df,
            'so_df': so_df
        }, f)
        
    return boq_df, so_df

def load_data():
    """
    Load data dari pickle file
    
    Returns:
        tuple: (boq_df, so_df) atau (None, None) jika file tidak ada
    """
    try:
        with open(DF_BASE_PATH, 'rb') as f:
            data = pickle.load(f)
            return data['boq_df'], data['so_df']
    except FileNotFoundError:
        return None, None
```

## 5. Implementasi Modul Transformasi Lainnya

Implementasikan file df_transformation lainnya dengan struktur yang konsisten:
- Input: data dari df_base.pkl
- Output: DataFrame hasil transformasi
- Fungsi untuk menyimpan hasil ke Excel di folder temp/

## 6. Fitur Expand/Minimize

Implementasikan fitur expand/minimize menggunakan komponen `st.expander`:

```python
with st.expander("BoQ Preview", expanded=True):
    st.dataframe(boq_df)
```

## 7. Fitur Upload to Odoo

Siapkan struktur untuk fitur yang akan diimplementasikan nanti:

```python
# Tombol Upload to Odoo (disabled untuk sekarang)
st.button("Upload to Odoo", disabled=True, help="Fitur ini akan diaktifkan pada tahap berikutnya")
```
