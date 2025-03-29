# Deskripsi Aplikasi Excel Converter

## Tujuan Aplikasi

Aplikasi Excel Converter adalah sebuah aplikasi berbasis web yang dirancang untuk memproses 2 file Excel berbeda (file BoQ dan file Convert to SO) dan mentransformasikannya menjadi 4 format Excel baru yang sesuai dengan format impor ke modul Odoo:

1. **ProductVariant** - Format untuk impor produk dan variannya ke Odoo
2. **BillOfMaterial** - Format untuk impor Bill of Material (BoM) ke Odoo
3. **SalesOrder** - Format untuk impor Sales Order ke Odoo
4. **UpdateProduct** - Format untuk update data produk di Odoo

## Proses Utama

Proses utama aplikasi terdiri dari 5 langkah:

1. **Ekstraksi & Penggabungan**: 
   - Mengupload dan mengekstrak data dari 2 file Excel input
   - Melakukan validasi silang antar kedua file
   - Menggabungkan kedua file menjadi 1 DataFrame dasar ("df-base")

2. **Transformasi ProductVariant**:
   - Memetakan "df-base" menjadi format "df-ProductVariant"
   - Memungkinkan user melihat dan mengedit hasilnya
   - Mengunduh dalam format Excel

3. **Transformasi BillOfMaterial**:
   - Memetakan "df-base" menjadi format "df-BillOfMaterial"
   - Memungkinkan user melihat dan mengedit hasilnya
   - Mengunduh dalam format Excel

4. **Transformasi SalesOrder**:
   - Memetakan "df-base" menjadi format "df-SalesOrder"
   - Memungkinkan user melihat dan mengedit hasilnya
   - Mengunduh dalam format Excel

5. **Transformasi UpdateProduct**:
   - Memetakan "df-base" menjadi format "df-UpdateProduct"
   - Memungkinkan user melihat dan mengedit hasilnya
   - Mengunduh dalam format Excel

## Fitur Utama

1. **Upload & Konfigurasi**:
   - Upload 2 file Excel berbeda
   - Konfigurasi parameter ekstraksi (sheet, header row, data start/end)
   - Pilih perusahaan (PT. Visiniaga Mitra Kreasindo atau CV. Kreasi Andalan Karya)

2. **Validasi Data**:
   - Validasi silang antara kolom "Description - Item yang Ditawarkan" pada file BoQ dan kolom "BOM Line" pada file Convert to SO
   - Menampilkan detail ketidakcocokan namun tetap memungkinkan proses lanjutan

3. **Transformasi Data**:
   - Penggabungan data dari kedua file input
   - Transformasi ke 4 format output yang berbeda
   - Preview data hasil transformasi

4. **Manipulasi Data**:
   - Tampilan tabel yang dapat diperbesar/diperkecil
   - Edit data sebelum diekspor
   - Filter dan pencarian data

5. **Export & Upload**:
   - Download hasil dalam format Excel
   - Persiapan untuk fitur upload langsung ke Odoo (akan diimplementasikan nanti)

## Teknologi

Aplikasi ini dikembangkan menggunakan:
- **Streamlit**: Framework Python untuk aplikasi data interaktif
- **Pandas**: Library manipulasi data
- **Library ETL**: Kumpulan fungsi ekstraksi, transformasi, dan loading data
- **Pickle**: Untuk penyimpanan data sementara
- **Integrasi Odoo**: Fitur masa depan untuk upload data langsung ke Odoo

## Target Pengguna

Aplikasi ini dirancang untuk digunakan oleh staf operasional yang perlu mengkonversi data dari format Excel standar ke format yang kompatibel dengan sistem Odoo, sehingga memudahkan proses impor data ke sistem ERP.
