# Planning UI/UX Aplikasi Excel Converter

## Struktur Folder
```
excel_converter/
│
├── ETL_library/              # Library ETL yang sudah ada
│   ├── extract.py            # Komponen ekstraksi data
│   ├── transform.py          # Komponen transformasi data
│   ├── load.py               # Komponen loading/export data
│   ├── validate.py           # Komponen validasi data
│   └── utility.py            # Fungsi-fungsi utilitas
│
├── df_transformation/        # Logic transformasi dataframe
│   ├── df_base.py            # Logic untuk dataframe dasar
│   ├── df_ProductVariant.py  # Logic untuk output ProductVariant
│   ├── df_BillOfMaterial.py  # Logic untuk output BillOfMaterial
│   ├── df_SalesOrder.py      # Logic untuk output SalesOrder
│   └── df_UpdateProduct.py   # Logic untuk output UpdateProduct
│
├── temp/                     # Folder untuk menyimpan file sementara
│   ├── df_base.pkl           # DataFrame base dalam format pickle
│   ├── output_products.xlsx  # Output Excel untuk ProductVariant
│   ├── output_bom.xlsx       # Output Excel untuk BillOfMaterial
│   ├── output_so.xlsx        # Output Excel untuk SalesOrder
│   └── output_update.xlsx    # Output Excel untuk UpdateProduct
│
├── main.py                   # Entry point aplikasi Streamlit
├── config.py                 # Konfigurasi aplikasi
│
└── requirements.txt          # Dependency aplikasi
```

## Struktur Utama
Aplikasi dibagi menjadi 3 tab utama yang mengikuti alur proses:

1. **Tab Upload & Konfigurasi**
2. **Tab Preview & Validasi**
3. **Tab Transformasi & Output**
   - **Sub-tab df-base**
     - Menampilkan 2 tabel preview terpisah (BoQ dan Convert to SO)
     - Setiap tabel memiliki fitur expand/minimize
     - Data langsung disimpan ke df_base.pkl
   - **Sub-tab Output**
     - df-ProductVariant (output 1)
     - df-BillOfMaterial (output 2)
     - df-SalesOrder (output 3)
     - df-UpdateProduct (output 4)
     - Semua tabel output dilengkapi fitur expand/minimize

Setiap sub-tab output (selain df-base) dilengkapi dengan tombol:
- Download Excel - untuk mengunduh hasil dalam format Excel
- Upload to Odoo - untuk mengupload data langsung ke Odoo (dinonaktifkan pada implementasi awal)

## Alur Pengguna
1. User mengupload file BoQ dan Convert to SO
2. User mengatur parameter (sheet, header row, data start/end row)
3. User memilih perusahaan
4. User klik "Validate Files"
5. Sistem menampilkan preview dan hasil validasi:
   - Jika ada ketidakcocokan, sistem menampilkan detail penyebabnya
   - User tetap dapat melanjutkan proses meski ada ketidakcocokan
6. User klik "Process Data" untuk melanjutkan
7. Sistem menampilkan "df-base" yang berisi 2 tabel preview terpisah:
   - Tabel preview data BoQ
   - Tabel preview data Convert to SO
8. User bisa beralih antara 5 sub-tab (df-base, df-ProductVariant, df-BillOfMaterial, df-SalesOrder, df-UpdateProduct)
9. User bisa:
   - Expand/minimize tabel untuk melihat data lebih detail
   - Melihat dan mengedit data pada setiap output
   - Mengunduh hasil dalam format Excel
   - Mengupload data ke Odoo langsung (setelah implementasi)
   - Sub-tab df-base (gabungan data dari input)
   - Sub-tab df-ProductVariant (output 1)
   - Sub-tab df-BillOfMaterial (output 2)
   - Sub-tab df-SalesOrder (output 3)
   - Sub-tab df-UpdateProduct (output 4)

Setiap sub-tab output (selain df-base) dilengkapi dengan tombol:
- Download Excel - untuk mengunduh hasil dalam format Excel
- Upload to Odoo - untuk mengupload data langsung ke Odoo (dinonaktifkan pada implementasi awal)

## 1. Tab Upload & Konfigurasi

```
+---------------------------------------------------------------------+
|                       EXCEL CONVERTER                               |
+---------------------------------------------------------------------+
| [Tab: Upload] [Tab: Preview] [Tab: Transform]                       |
+---------------------------------------------------------------------+
|                                                                     |
|  +----------------------------+  +----------------------------+     |
|  | Upload BoQ File            |  | Upload Convert to SO File  |     |
|  | [Drop file here]           |  | [Drop file here]           |     |
|  | [Browse File]              |  | [Browse File]              |     |
|  +----------------------------+  +----------------------------+     |
|                                                                     |
|  +----------------------------+  +----------------------------+     |
|  | BoQ Settings               |  | Convert to SO Settings     |     |
|  | -------------------------- |  | -------------------------- |     |
|  | Sheet: [Dropdown]          |  | Sheet: [Dropdown]          |     |
|  | Header Row: [Input field]  |  | Header Row: [Input field]  |     |
|  | Data Start: [Input field]  |  | Data Start: [Input field]  |     |
|  | Data End: [Input field]    |  | Data End: [Input field]    |     |
|  +----------------------------+  +----------------------------+     |
|                                                                     |
|  Company: [PT. Visiniaga Mitra Kreasindo ⬇️]                       |
|                                                                     |
|  [Validate Files]                                                   |
|                                                                     |
+---------------------------------------------------------------------+
```

## 2. Tab Preview & Validasi

```
+---------------------------------------------------------------------+
|                       EXCEL CONVERTER                               |
+---------------------------------------------------------------------+
| [Tab: Upload] [Tab: Preview] [Tab: Transform]                       |
+---------------------------------------------------------------------+
|                                                                     |
|  Validation Results: ⚠️ Mismatch detected                           |
|  Detail ketidakcocokan:                                             |
|  - Value 'Item A' in BoQ.Description tidak ditemukan di BOM Line    |
|  - Value 'Product X' in BOM Line tidak ditemukan di Description     |
|  [Lihat Detail Lengkap ▼]                                           |
|                                                                     |
|  +----------------------------+  +----------------------------+     |
|  | BoQ Preview                |  | Convert to SO Preview      |     |
|  | [Tabel data dengan scroll] |  | [Tabel data dengan scroll] |     |
|  +----------------------------+  +----------------------------+     |
|                                                                     |
|  [Back]                           [Process Data]                    |
|                                                                     |
|  Note: Anda masih dapat melanjutkan proses meskipun ada mismatch   |
|                                                                     |
+---------------------------------------------------------------------+
```

## 3. Tab Transformasi & Output - df-base

```
+---------------------------------------------------------------------+
|                       EXCEL CONVERTER                               |
+---------------------------------------------------------------------+
| [Tab: Upload] [Tab: Preview] [Tab: Transform]                       |
+---------------------------------------------------------------------+
|                                                                     |
|  [df-base] [df-ProductVariant] [df-BillOfMaterial] [df-SalesOrder] [df-UpdateProduct]
|                                                                     |
|  +---------------------------------------------+                    |
|  | BoQ Preview                  [↕ Expand/Minimize]                 |
|  +---------------------------------------------+                    |
|  | [Tabel data preview dari file BoQ]                               |
|  | [Konten dapat di-scroll horizontal & vertikal]                   |
|  | [Filter & pencarian]                                             |
|  +---------------------------------------------+                    |
|                                                                     |
|  +---------------------------------------------+                    |
|  | Convert to SO Preview        [↕ Expand/Minimize]                 |
|  +---------------------------------------------+                    |
|  | [Tabel data preview dari file Convert to SO]                     |
|  | [Konten dapat di-scroll horizontal & vertikal]                   |
|  | [Filter & pencarian]                                             |
|  +---------------------------------------------+                    |
|                                                                     |
|  [Back]        [Download Excel]                                     |
|                                                                     |
+---------------------------------------------------------------------+
```

## 3. Tab Transformasi & Output - Tabs Lainnya

```
+---------------------------------------------------------------------+
|                       EXCEL CONVERTER                               |
+---------------------------------------------------------------------+
| [Tab: Upload] [Tab: Preview] [Tab: Transform]                       |
+---------------------------------------------------------------------+
|                                                                     |
|  [df-base] [df-ProductVariant] [df-BillOfMaterial] [df-SalesOrder] [df-UpdateProduct]
|                                                                     |
|  +---------------------------------------------+                    |
|  | [Output Preview]            [↕ Expand/Minimize]                  |
|  +---------------------------------------------+                    |
|  | [Tabel data dengan preview output yang dipilih]                  |
|  | [Konten dapat di-scroll horizontal & vertikal]                   |
|  | [Tampilan data lengkap dengan paginasi]                          |
|  | [Filter & pencarian kolom]                                       |
|  +---------------------------------------------+                    |
|                                                                     |
|  [Back]        [Download Excel]        [Upload to Odoo]*            |
|                                                                     |
|  * Tombol "Upload to Odoo" hanya aktif di tab selain df-base        |
|    (Tombol ini akan dinonaktifkan sampai implementasi Odoo selesai) |
|                                                                     |
+---------------------------------------------------------------------+
```

## Struktur Utama
Aplikasi dibagi menjadi 3 tab utama yang mengikuti alur proses:

1. **Tab Upload & Konfigurasi**
2. **Tab Preview & Validasi**
3. **Tab Transformasi & Output**
   - **Sub-tab df-base**
     - Menampilkan 2 tabel preview terpisah (BoQ dan Convert to SO)
     - Setiap tabel memiliki fitur expand/minimize
     - Data langsung disimpan ke df_base.pkl
   - **Sub-tab Output**
     - df-ProductVariant (output 1)
     - df-BillOfMaterial (output 2)
     - df-SalesOrder (output 3)
     - df-UpdateProduct (output 4)
     - Semua tabel output dilengkapi fitur expand/minimize

Setiap sub-tab output (selain df-base) dilengkapi dengan tombol:
- Download Excel - untuk mengunduh hasil dalam format Excel
- Upload to Odoo - untuk mengupload data langsung ke Odoo (dinonaktifkan pada implementasi awal)

## Fitur Tambahan
- Tooltip bantuan untuk setiap input parameter
- Indikator loading saat memproses data
- Highlight untuk nilai yang tidak valid
- Pesan error yang jelas dan informatif
- Fitur pencarian dan filter pada tabel "df-base" untuk mengelola data yang banyak
- Paginasi pada tabel data dengan kontrol jumlah baris per halaman
- Kemampuan resize kolom tabel untuk melihat data panjang
- Fitur pengurutan (sorting) berdasarkan kolom
- Fitur pencarian dan filter pada tabel "df-base" untuk mengelola data yang banyak
- Paginasi pada tabel data dengan kontrol jumlah baris per halaman
- Kemampuan resize kolom tabel untuk melihat data panjang
- Fitur pengurutan (sorting) berdasarkan kolom
