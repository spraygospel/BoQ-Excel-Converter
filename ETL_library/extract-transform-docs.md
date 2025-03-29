# Dokumentasi Komponen ETL OdooBatch

Dokumen ini berisi penjelasan detail tentang komponen Extract dan Transform yang telah dibuat sebagai bagian dari platform ETL OdooBatch. Komponen-komponen ini dirancang untuk memproses file Excel/CSV dan mentransformasikannya menjadi data yang siap diimpor ke Odoo.

## 1. Komponen Extract (extract.py)

### 1.1 ExcelExtractor

`ExcelExtractor` adalah kelas utama untuk mengekstrak data dari file Excel dengan kemampuan mendeteksi range data berdasarkan border dan parameter lainnya.

#### Parameter:

- `file_path`: Path ke file Excel yang akan dibaca
- `sheet_name`: Nama atau indeks sheet (default: 0)
- `header_row`: Nomor baris header (1-based)
- `data_start_row`: Nomor baris awal data (1-based)
- `data_end_row`: Nomor baris akhir data (opsional)
- `header_start_col`: Kolom awal header (opsional)
- `header_end_col`: Kolom akhir header (opsional)
- `auto_detect_range`: Boolean untuk mengaktifkan/menonaktifkan deteksi otomatis (default: True)

#### Fitur Utama:

- **Deteksi Border Otomatis**: Dapat mendeteksi akhir tabel berdasarkan bottom border pada sel
- **Extraksi Header & Data**: Mengekstrak header dan data dari range yang ditentukan
- **Penanganan Multi-Sheet**: Dapat bekerja dengan sheet name atau index
- **Fleksibilitas Range**: Memungkinkan penentuan range secara manual atau otomatis

#### Contoh Penggunaan:

```python
extractor = ExcelExtractor(
    file_path="data.xlsx",
    sheet_name="Sheet1",
    header_row=5,
    data_start_row=6,
    auto_detect_range=True
)
df, border_info = extractor.extract()
```

### 1.2 CSVExtractor

`CSVExtractor` adalah kelas untuk mengekstrak data dari file CSV.

#### Parameter:

- `file_path`: Path ke file CSV yang akan dibaca
- `encoding`: Encoding file CSV (default: "utf-8")
- `header_row`: Nomor baris header (0-based)
- `data_start_row`: Nomor baris awal data (0-based)
- `data_end_row`: Nomor baris akhir data (opsional)

#### Fitur Utama:

- **Ekstraksi Header & Data**: Mengekstrak header dan data dari range yang ditentukan
- **Penanganan Encoding**: Dapat menangani berbagai jenis encoding

#### Contoh Penggunaan:

```python
extractor = CSVExtractor(
    file_path="data.csv",
    encoding="utf-8",
    header_row=0,
    data_start_row=1
)
df = extractor.extract()
```

### 1.3 create_extractor (Factory Function)

Fungsi factory untuk membuat extractor yang sesuai berdasarkan tipe file.

#### Parameter:

Sama dengan parameter untuk `ExcelExtractor` dan `CSVExtractor`.

#### Contoh Penggunaan:

```python
extractor = create_extractor(
    file_path="data.xlsx",
    header_row=5,
    data_start_row=6
)
df, info = extractor.extract()
```

## 2. Komponen Transform (transform.py)

### 2.1 WhitespaceCleaner

Kelas untuk membersihkan baris dan kolom kosong (whitespace) dari DataFrame.

#### Parameter:

- `clean_rows`: Boolean untuk membersihkan baris kosong (default: True)
- `clean_cols`: Boolean untuk membersihkan kolom kosong (default: True)
- `threshold`: Persentase sel yang harus kosong untuk dianggap kosong (default: 0.9)

#### Fitur Utama:

- **Pembersihan Baris**: Menghapus baris yang sebagian besar kosong
- **Pembersihan Kolom**: Menghapus kolom yang sebagian besar kosong
- **Threshold Konfigurasi**: Dapat mengatur ambang batas untuk dianggap kosong

#### Contoh Penggunaan:

```python
cleaner = WhitespaceCleaner(
    clean_rows=True,
    clean_cols=True,
    threshold=0.8
)
cleaned_df = cleaner.clean(df)
```

### 2.2 EmptyspaceCleaner

Kelas untuk menghapus baris yang memiliki cell kosong pada kolom tertentu.

#### Parameter:

- `header_names`: Nama kolom atau list nama kolom yang tidak boleh kosong

#### Fitur Utama:

- **Multi-Column Validation**: Dapat melakukan validasi pada beberapa kolom sekaligus
- **Strict Cleaning**: Menghapus baris jika ada nilai kosong di kolom yang ditentukan

#### Contoh Penggunaan:

```python
cleaner = EmptyspaceCleaner(
    header_names=["Product Code", "Description"]
)
cleaned_df = cleaner.clean(df)
```

### 2.3 SectionExtractor

Kelas untuk mengekstrak section header ke kolom terpisah dan mengkategorikan baris.

#### Parameter:

- `section_indicator_col`: Kolom yang mengidentifikasi section header
- `target_section_col`: Nama kolom baru untuk nilai section
- `remove_section_rows`: Boolean untuk menghapus baris section header (default: True)

#### Fitur Utama:

- **Deteksi Section**: Mengidentifikasi section berdasarkan ciri-ciri (nilai tidak kosong di kolom indikator, cell di sekitar kosong)
- **Kategorisasi Baris**: Mengelompokkan baris berdasarkan section
- **Pemindahan Section**: Memindahkan nilai section ke kolom baru untuk setiap baris di bawahnya

#### Contoh Penggunaan:

```python
extractor = SectionExtractor(
    section_indicator_col="Description",
    target_section_col="Section"
)
sectioned_df = extractor.extract(df)
```

### 2.4 FieldMapper

Kelas untuk memetakan kolom sumber ke field target dengan nama baru.

#### Parameter:

- `mapping`: Dictionary mapping dari {kolom_sumber: kolom_target}
- `transform_functions`: Dictionary {kolom_target: fungsi_transformasi} (opsional)
- `default_values`: Dictionary {kolom_target: nilai_default} (opsional)

#### Fitur Utama:

- **Selective Column Mapping**: Hanya mengambil kolom yang diperlukan dari DataFrame sumber
- **Column Renaming**: Mengubah nama kolom sesuai kebutuhan
- **Data Transformation**: Dapat menerapkan fungsi transformasi pada nilai
- **Default Values**: Menyediakan nilai default jika kolom sumber tidak ada

#### Contoh Penggunaan:

```python
mapper = FieldMapper(
    mapping={
        "description": "Name",
        "quantity": "Jumlah",
        "supplier": "Vendor"
    },
    default_values={
        "Jumlah": 0,
        "Vendor": "Unknown"
    }
)
mapped_df = mapper.map_fields(df)
```

### 2.5 DataFrameJoiner

Kelas untuk menggabungkan dua DataFrame berdasarkan kolom kunci yang sama/terkait.

#### Parameter:

- `left_key`: Kolom kunci di DataFrame kiri (utama)
- `right_key`: Kolom kunci di DataFrame kanan
- `join_type`: Tipe join ("left", "right", "inner", "outer") (default: "left")
- `columns_to_add`: Kolom-kolom dari DataFrame kanan yang ingin ditambahkan (opsional)
- `target_column_names`: Nama baru untuk kolom yang ditambahkan (opsional)
- `match_case`: Boolean untuk case-sensitive matching (default: False)

#### Fitur Utama:

- **Flexible Join Types**: Mendukung berbagai jenis join (left, right, inner, outer)
- **Selective Column Addition**: Dapat memilih kolom spesifik untuk ditambahkan
- **Case-Insensitive Matching**: Opsional untuk non-case-sensitive matching
- **Column Renaming**: Dapat mengubah nama kolom saat ditambahkan

#### Contoh Penggunaan:

```python
joiner = DataFrameJoiner(
    left_key="Product_Code",
    right_key="Code",
    join_type="left",
    columns_to_add=["Price", "Supplier"],
    match_case=False
)
joined_df = joiner.join(df1, df2)
```

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

### 2.11 DataFrameSorter

Kelas untuk mengurutkan DataFrame berdasarkan satu atau beberapa kolom dengan berbagai opsi kontrol.

#### Parameter:

- `sort_columns`: Kolom untuk pengurutan (string, list, atau dictionary)
- `ascending`: Boolean atau list boolean untuk menentukan arah pengurutan (default: True)
- `na_position`: Posisi nilai null ('first' atau 'last', default: 'last')
- `reset_index`: Boolean untuk reset index setelah pengurutan (default: True)
- `custom_order`: Dictionary untuk pengurutan kustom dengan format {kolom: [daftar_nilai]}

#### Fitur Utama:

- **Multi-Column Sorting**: Dapat mengurutkan berdasarkan beberapa kolom sekaligus
- **Flexible Direction**: Kontrol arah pengurutan (ascending/descending) per kolom
- **Custom Ordering**: Mendukung pengurutan kustom berdasarkan daftar nilai yang ditentukan
- **Null Handling**: Kontrol penempatan nilai null dalam hasil pengurutan

#### Contoh Penggunaan:

```python
# Pengurutan sederhana berdasarkan satu kolom
sorter = DataFrameSorter(
    sort_columns="age",
    ascending=False  # Descending order
)
sorted_df = sorter.transform(df)

# Pengurutan berdasarkan beberapa kolom
sorter = DataFrameSorter(
    sort_columns=["name", "age"],
    ascending=[True, False]  # name ascending, age descending
)
sorted_df = sorter.transform(df)

# Pengurutan dengan custom order
sorter = DataFrameSorter(
    sort_columns="level",
    custom_order={"level": ["beginner", "intermediate", "advanced", "master"]}
)
sorted_df = sorter.transform(df)

# Pengurutan kompleks menggunakan metode kelas
sorted_df = DataFrameSorter.sort_by_multiple(
    df,
    sort_spec={
        "name": True,  # name ascending
        "level": {
            "ascending": True,
            "custom_order": ["beginner", "intermediate", "advanced", "master"]
        }
    }
)
```

## 3. Komponen Validate (validate.py)

### 3.1 DataValidator

`DataValidator` adalah kelas untuk memvalidasi data berdasarkan aturan bisnis yang ditentukan.

#### Parameter:

- `validation_rules`: Dictionary aturan validasi per kolom
- `error_handling`: Strategi penanganan error ("fail", "warn", "ignore") (default: "warn")

#### Fitur Utama:

- **Flexible Rules**: Mendukung berbagai jenis aturan validasi (not_null, min_value, max_value, regex, in_list, custom)
- **Error Handling**: Tiga strategi penanganan error (fail, warn, ignore)
- **Detailed Reports**: Laporan validasi terperinci dengan lokasi dan deskripsi masalah
- **Custom Validation**: Mendukung fungsi validasi kustom

#### Contoh Penggunaan:

```python
validator = DataValidator(
    validation_rules={
        'Product': [
            {'type': 'not_null', 'message': 'Nama produk tidak boleh kosong'}
        ],
        'Quantity': [
            {'type': 'min_value', 'value': 0, 'message': 'Kuantitas harus >= 0'},
            {'type': 'max_value', 'value': 1000, 'message': 'Kuantitas harus <= 1000'}
        ],
        'Category': [
            {'type': 'in_list', 'values': ['A', 'B', 'C'], 'message': 'Kategori harus A, B, atau C'}
        ],
        'Code': [
            {'type': 'regex', 'pattern': '^[A-Z]{2}-\d{4}$', 'message': 'Format kode tidak valid'}
        ]
    },
    error_handling="warn"
)
result_df, validation_report = validator.validate(df)
```

### 3.2 CrossFileValidator

`CrossFileValidator` adalah kelas untuk memvalidasi data antar file berbeda.

#### Parameter:

- `error_handling`: Strategi penanganan error ("fail", "warn", "ignore") (default: "warn")

#### Fitur Utama:

- **Cross-File Validation**: Memvalidasi kecocokan data antar file berbeda
- **Detailed Mismatch Reports**: Laporan terperinci tentang data yang tidak cocok
- **Case Sensitivity Control**: Kontrol case sensitivity dalam pencocokan
- **Custom File Labeling**: Penamaan khusus untuk file dalam laporan

#### Contoh Penggunaan:

```python
validator = CrossFileValidator(error_handling="warn")
is_match, validation_report = validator.validate_matching_values(
    first_df=products_df,
    second_df=inventory_df,
    first_col="product_code",
    second_col="item_code",
    case_sensitive=False,
    label_first="Products",
    label_second="Inventory"
)
```

## 4. Komponen Load (load.py)

### 4.1 OdooConnector

`OdooConnector` adalah kelas untuk menangani koneksi ke Odoo melalui XML-RPC dan melakukan operasi CRUD.

#### Parameter:

- `host`: URL server Odoo (contoh: https://example.com)
- `db`: Nama database
- `username`: Username
- `password`: Password
- `timeout`: Timeout koneksi dalam detik (default: 120)

#### Fitur Utama:

- **XML-RPC Connection**: Koneksi ke Odoo melalui protokol XML-RPC
- **Authentication**: Autentikasi dan manajemen session
- **CRUD Operations**: Operasi Create, Read, Update, Delete
- **Bulk Operations**: Mendukung operasi bulk untuk performa yang lebih baik
- **Error Handling**: Penanganan error yang komprehensif dengan logging

#### Contoh Penggunaan:

```python
# Inisialisasi koneksi
connector = OdooConnector(
    host="https://erp.example.com",
    db="production_db",
    username="admin",
    password="secure_password"
)

# Connect ke Odoo
connected = connector.connect()

if connected:
    # Mencari produk
    product_ids = connector.search(
        model="product.product",
        domain=[('type', '=', 'product')],
        limit=10
    )
    
    # Membaca detail produk
    products = connector.read(
        model="product.product",
        record_ids=product_ids,
        fields=['name', 'default_code', 'list_price']
    )
    
    # Membuat pesanan penjualan
    order_id = connector.create(
        model="sale.order",
        values={
            'partner_id': 1,
            'date_order': '2023-01-15',
            'order_line': [
                (0, 0, {
                    'product_id': product_ids[0],
                    'product_uom_qty': 5
                })
            ]
        }
    )
    
    # Bulk create dari DataFrame
    created_ids, errors = connector.bulk_create(
        model="product.product",
        df=products_df,
        chunk_size=100
    )
```

## 5. Penggunaan Komponen dalam Pipeline ETL

### 5.1 Pipeline Validasi dan Pemuatan:

```python
# 1. Extract data dari Excel
extractor = create_extractor(
    file_path="input.xlsx",
    header_row=5,
    data_start_row=6
)
df, _ = extractor.extract()

# 2. Bersihkan data
whitespace_cleaner = WhitespaceCleaner(threshold=0.8)
df = whitespace_cleaner.clean(df)

# 3. Validasi data
validator = DataValidator(
    validation_rules={
        'Product': [
            {'type': 'not_null', 'message': 'Nama produk tidak boleh kosong'}
        ],
        'Quantity': [
            {'type': 'min_value', 'value': 0, 'message': 'Kuantitas harus >= 0'}
        ]
    }
)
validated_df, validation_report = validator.validate(df)

# 4. Validasi cross-file (jika diperlukan)
if reference_df is not None:
    cross_validator = CrossFileValidator()
    is_match, cross_validation_report = cross_validator.validate_matching_values(
        first_df=validated_df,
        second_df=reference_df,
        first_col="Product",
        second_col="product_name"
    )

# 5. Map fields ke format Odoo
mapper = FieldMapper(
    mapping={
        "Product": "name",
        "Quantity": "qty_available",
        "Price": "list_price"
    }
)
odoo_df = mapper.map_fields(validated_df)

# 6. Koneksi ke Odoo
connector = OdooConnector(
    host="https://erp.example.com",
    db="production_db",
    username="admin",
    password="secure_password"
)
connected = connector.connect()

# 7. Import data ke Odoo
if connected:
    created_ids, errors = connector.bulk_create(
        model="product.product",
        df=odoo_df,
        chunk_size=50
    )
    
    print(f"Successfully created {len(created_ids)} products")
    if errors:
        print(f"Encountered {len(errors)} errors during import")
```

### 5.2 Penggunaan Lanjutan OdooConnector:

```python
# Koneksi ke Odoo
connector = OdooConnector(
    host="https://erp.example.com",
    db="production_db",
    username="admin",
    password="secure_password"
)
connected = connector.connect()

if connected:
    # 1. Baca data master dari Odoo untuk referensi
    partner_ids = connector.search(
        model="res.partner",
        domain=[('customer_rank', '>', 0)],
        limit=1000
    )
    partners = connector.read(
        model="res.partner",
        record_ids=partner_ids,
        fields=['id', 'name', 'email']
    )
    partners_df = pd.DataFrame(partners)
    
    # 2. Gunakan data Odoo untuk pengayaan data lokal
    joiner = DataFrameJoiner(
        left_key="Customer",
        right_key="name",
        join_type="left",
        columns_to_add=["id", "email"],
        target_column_names={"id": "partner_id", "email": "customer_email"},
        match_case=False
    )
    enriched_df = joiner.join(sales_df, partners_df)
    
    # 3. Persiapkan data pesanan penjualan
    order_lines = []
    for _, row in enriched_df.iterrows():
        # Cari produk
        product_ids = connector.search(
            model="product.product",
            domain=[('default_code', '=', row['Product_Code'])],
            limit=1
        )
        
        if product_ids:
            # Tambahkan baris pesanan
            order_lines.append((0, 0, {
                'product_id': product_ids[0],
                'product_uom_qty': row['Quantity'],
                'price_unit': row['Unit_Price']
            }))
    
    # 4. Buat pesanan penjualan
    if order_lines:
        order_id = connector.create(
            model="sale.order",
            values={
                'partner_id': enriched_df.iloc[0]['partner_id'],
                'date_order': datetime.datetime.now().strftime('%Y-%m-%d'),
                'order_line': order_lines
            }
        )
        print(f"Created sale order with ID: {order_id}")
```

## 6. Tips dan Praktik Terbaik

### 6.1 Validasi Data

1. **Multi-Level Validation**: Gunakan validasi bertingkat - awalnya dengan "warn" untuk melihat masalah, kemudian dengan "fail" untuk strict enforcement.

2. **Validasi Cross-File**: Pastikan untuk memvalidasi referensi antar file sebelum pengimporan untuk menghindari foreign key errors.

3. **Custom Validators**: Buat fungsi validasi kustom untuk aturan bisnis kompleks yang tidak dapat ditangani oleh validator standar.

4. **Validasi Sebelum Transformasi**: Lakukan validasi dasar sebelum transformasi untuk menghindari error selama proses transformasi.

### 6.2 Koneksi Odoo

1. **Connection Pooling**: Gunakan satu instance OdooConnector untuk beberapa operasi alih-alih membuat koneksi baru setiap kali.

2. **Chunking**: Gunakan `bulk_create` dengan `chunk_size` yang sesuai untuk menghindari timeout pada operasi besar.

3. **Error Handling**: Selalu periksa `errors` yang dikembalikan oleh `bulk_create` untuk menangani kegagalan parsial.

4. **Credential Security**: Jangan hardcode kredensial, gunakan environment variables atau secure configuration files.

5. **Timeouts**: Sesuaikan timeout berdasarkan ukuran operasi dan kecepatan jaringan Anda.

### 6.3 Penggunaan Pipeline

1. **Modular Pipelines**: Bagi pipeline menjadi fungsi yang lebih kecil dan dapat digunakan kembali.

2. **Logging**: Terapkan logging yang komprehensif untuk semua tahap ETL.

3. **Handling Exceptions**: Gunakan try-except dan "error_handling" parameter untuk mencegah kegagalan pipeline total.

4. **Checkpoint Files**: Simpan data antara setelah tahap-tahap kritis untuk memungkinkan resume jika terjadi kegagalan.

5. **Parallel Processing**: Untuk file sangat besar, pertimbangkan untuk memproses secara paralel dengan membagi data.