# OdooBatch ETL Component Library

Dokumentasi ini berisi daftar komponen yang tersedia dalam OdooBatch ETL Platform untuk membangun workflow ETL dari Excel/CSV ke Odoo.

## 1. Extract Components

### 1.1 FileReader
**Deskripsi**: Komponen dasar untuk membaca file Excel, CSV, atau PDF.
**Input**: File path
**Output**: DataFrame mentah
**Parameter**:
- `file_path`: Path ke file yang akan dibaca
- `file_type`: Tipe file ("excel", "csv", "pdf")
- `sheet_name`: Nama sheet untuk Excel (default: 0)
- `encoding`: Encoding untuk file CSV (default: "utf-8")

### 1.2 DataRangeSelector
**Deskripsi**: Menentukan range data yang valid dalam file (header row, data start/end).
**Input**: DataFrame mentah
**Output**: DataFrame dengan range terfilter
**Parameter**:
- `header_row`: Nomor baris yang berisi header (0-based)
- `data_start_row`: Nomor baris awal data (0-based)
- `data_end_row`: Nomor baris akhir data (opsional)
- `header_start_col`: Kolom awal header (opsional)
- `header_end_col`: Kolom akhir header (opsional)

## 2. Transform Components

### 2.1 WhitespaceCleaner
**Deskripsi**: Menghapus whitespace (baris/kolom kosong) dari DataFrame.
**Input**: DataFrame
**Output**: DataFrame dibersihkan
**Parameter**:
- `clean_rows`: Boolean untuk membersihkan baris kosong (default: True)
- `clean_cols`: Boolean untuk membersihkan kolom kosong (default: True)
- `threshold`: Persentase sel yang harus kosong untuk dianggap kosong (default: 0.9)

### 2.2 EmptyspaceCleaner
**Deskripsi**: Menghapus empty space (baris/kolom kosong) dari DataFrame berdasarkan jika ada cell di suatu kolom yang kosong.
**Input**: DataFrame
**Output**: DataFrame dibersihkan
**Parameter**:
- `header_name`: row dari header mana yang akan dilakukan cek.

### 2.3 SectionExtractor
**Deskripsi**: Mengekstrak section header ke kolom terpisah dan mengkategorikan baris.
**Input**: DataFrame dengan section teridentifikasi
**Output**: DataFrame dengan kolom section baru
**Parameter**:
- `section_indicator_col`: Kolom yang mengidentifikasi section header
- `target_section_col`: Nama kolom baru untuk nilai section
- `remove_section_rows`: Boolean untuk menghapus baris section header (default: True)

### 2.4 FieldMapper
**Deskripsi**: Memetakan kolom sumber ke field target Odoo.
**Input**: DataFrame sumber
**Output**: DataFrame dengan struktur target
**Parameter**:
- `mapping`: Dictionary dari kolom sumber ke field target
- `transform_functions`: Dictionary dari field target ke fungsi transformasi
- `default_values`: Dictionary nilai default untuk field target

### 2.5 DataFrameJoiner
**Deskripsi**: Menggabungkan dua DataFrame berdasarkan kolom kunci yang sama/terkait.
**Input**: Dua DataFrame
**Output**: DataFrame gabungan
**Parameter**:
- `left_df_id`: Identifier untuk DataFrame kiri (utama)
- `right_df_id`: Identifier untuk DataFrame kanan
- `left_key`: Kolom kunci di DataFrame kiri
- `right_key`: Kolom kunci di DataFrame kanan
- `join_type`: Tipe join ("left", "right", "inner", "outer")
- `columns_to_add`: Kolom-kolom dari DataFrame kanan yang ingin ditambahkan
- `target_column_names`: Nama baru untuk kolom yang ditambahkan (opsional)
- `match_case`: Boolean untuk case-sensitive matching (default: False)

### 2.6 DuplicateSuppressor

Kelas untuk menghilangkan tampilan nilai duplikat pada kolom tertentu untuk meningkatkan keterbacaan.

#### Parameter:

- `columns_to_suppress`: Kolom atau list kolom yang akan dikosongkan jika terdapat duplikat

#### Fitur Utama:

- **Peningkatan Keterbacaan**: Menghilangkan tampilan nilai yang berulang pada kolom tertentu
- **Multi-Column Support**: Dapat menangani multiple kolom sekaligus
- **Preservasi Data Asli**: Hanya mengosongkan tampilan tanpa menghapus data sebenarnya

#### Contoh Penggunaan:

```python
suppressor = DuplicateSuppressor(
    columns_to_suppress=["name", "age", "nationality"]
)
clean_df = suppressor.transform(df)
```

### 2.7 DuplicateRestorer

Kelas untuk mengisi kembali nilai kosong dengan nilai terakhir yang tidak kosong pada kolom tertentu.

#### Parameter:

- `columns_to_restore`: Kolom atau list kolom yang akan diisi dengan nilai terakhir yang tidak kosong

#### Fitur Utama:

- **Pemulihan Data**: Mengisi nilai kosong dengan nilai terakhir yang valid
- **Multi-Column Support**: Dapat memulihkan beberapa kolom sekaligus
- **Penanganan NA**: Menangani data NA dan string kosong dengan benar

#### Contoh Penggunaan:

```python
restorer = DuplicateRestorer(
    columns_to_restore=["name", "age", "nationality"]
)
restored_df = restorer.transform(df)
```

### 2.8 TextPrefixFormatter

Kelas untuk memastikan semua nilai di kolom tertentu memiliki awalan teks yang seragam.

#### Parameter:

- `column`: Kolom yang akan diformat
- `prefix`: Awalan teks yang akan ditambahkan jika belum ada
- `skip_na`: Boolean untuk melewati nilai NA (default: True)

#### Fitur Utama:

- **Format Seragam**: Memastikan semua nilai di kolom memiliki awalan yang sama
- **Pencegahan Duplikasi**: Hanya menambahkan awalan jika belum ada
- **Kontrol NA**: Pengaturan apakah melewati atau memformat nilai NA

#### Contoh Penggunaan:

```python
formatter = TextPrefixFormatter(
    column="skills",
    prefix="R - ",
    skip_na=True
)
formatted_df = formatter.transform(df)
```

### 2.9 StaticFieldAdder

Kelas untuk menambahkan kolom baru dengan nilai statis ke DataFrame.

#### Parameter:

- `fields_to_add`: Dictionary dengan format {nama_kolom: nilai_statis}

#### Fitur Utama:

- **Penambahan Multi-Kolom**: Dapat menambahkan beberapa kolom statis sekaligus
- **Validasi Kolom**: Memeriksa apakah kolom sudah ada sebelum menambahkan
- **Fleksibilitas Nilai**: Mendukung berbagai tipe data untuk nilai statis

#### Contoh Penggunaan:

```python
adder = StaticFieldAdder(
    fields_to_add={
        "number": 3,
        "company": "ACME Corp",
        "active": True
    }
)
expanded_df = adder.transform(df)
```

### 2.10 ColumnReorderer

Kelas untuk mengubah urutan kolom DataFrame sesuai dengan urutan yang diinginkan.

#### Parameter:

- `column_order`: List urutan kolom yang diinginkan
- `include_remaining`: Boolean untuk menyertakan kolom yang tidak disebutkan di akhir (default: True)

#### Fitur Utama:

- **Pengaturan Urutan**: Mengatur ulang kolom sesuai urutan yang diinginkan
- **Kontrol Kolom Tersisa**: Opsi untuk menyertakan atau mengabaikan kolom yang tidak disebutkan
- **Validasi Kolom**: Memeriksa keberadaan kolom yang diinginkan

#### Contoh Penggunaan:

```python
reorderer = ColumnReorderer(
    column_order=["age", "name", "nationality", "number", "skills", "level"],
    include_remaining=True
)
reordered_df = reorderer.transform(df)
```

## 3. Validation Components

### 3.1 DataValidator
**Deskripsi**: Memvalidasi data berdasarkan aturan bisnis.
**Input**: DataFrame
**Output**: DataFrame + laporan validasi
**Parameter**:
- `validation_rules`: Dictionary aturan validasi per kolom
- `error_handling`: Strategi penanganan error ("fail", "warn", "ignore")

### 3.2 CrossFileValidator
**Deskripsi**: Memvalidasi data antar file berbeda.
**Input**: List dari DataFrames
**Output**: Laporan validasi
**Parameter**:
- `validation_rules`: List aturan validasi antar file
- `file_identifiers`: Identifier untuk file
- `key_cols`: Kolom kunci untuk validasi

## 4. Load Components

### 4.1 OdooConnector
**Deskripsi**: Menangani koneksi ke Odoo melalui XML-RPC.
**Input**: Konfigurasi koneksi
**Output**: Instance koneksi
**Parameter**:
- `host`: URL server Odoo
- `db`: Nama database
- `username`: Username
- `password`: Password
- `timeout`: Timeout koneksi (detik)

## 5. Utility Components

### 5.1 DataPreview
**Deskripsi**: Menghasilkan preview data untuk UI.
**Input**: DataFrame
**Output**: Preview data untuk tampilan
**Parameter**:
- `max_rows`: Jumlah baris untuk preview
- `include_stats`: Boolean untuk menyertakan statistik