import pandas as pd
import numpy as np
from typing import Dict, List, Callable, Optional, Union, Any, Tuple


class WhitespaceCleaner:
    """
    Menghapus whitespace (baris/kolom kosong) dari DataFrame.
    """
    
    def __init__(
        self,
        clean_rows: bool = True,
        clean_cols: bool = True,
        threshold: float = 0.9
    ):
        self.clean_rows = clean_rows
        self.clean_cols = clean_cols
        self.threshold = threshold
    
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Membersihkan baris dan kolom kosong dari DataFrame."""
        result_df = df.copy()
        
        # Bersihkan baris kosong
        if self.clean_rows:
            # Hitung persentase nilai NaN atau string kosong di setiap baris
            row_null_count = result_df.apply(
                lambda row: row.isna().sum() + sum(1 for x in row if isinstance(x, str) and not x.strip()),
                axis=1
            )
            row_null_percent = row_null_count / len(result_df.columns)
            
            # Filter baris yang memiliki nilai kosong lebih sedikit dari threshold
            result_df = result_df[row_null_percent < self.threshold]
        
        # Bersihkan kolom kosong
        if self.clean_cols:
            # Hitung persentase nilai NaN atau string kosong di setiap kolom
            col_null_count = result_df.apply(
                lambda col: col.isna().sum() + sum(1 for x in col if isinstance(x, str) and not x.strip()),
                axis=0
            )
            col_null_percent = col_null_count / len(result_df)
            
            # Filter kolom yang memiliki nilai kosong lebih sedikit dari threshold
            result_df = result_df.loc[:, col_null_percent < self.threshold]
        
        # Reset index setelah filtering
        return result_df.reset_index(drop=True)


class EmptyspaceCleaner:
    """
    Menghapus baris dari DataFrame jika ada cell kosong di kolom yang ditentukan.
    """
    
    def __init__(self, header_names: Union[str, List[str]]):
        """
        Inisialisasi dengan nama kolom yang akan dicek.
        
        Args:
            header_names: Nama kolom atau list nama kolom yang tidak boleh kosong
        """
        if isinstance(header_names, str):
            self.header_names = [header_names]
        else:
            self.header_names = header_names
    
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Membersihkan baris yang memiliki cell kosong pada kolom yang ditentukan."""
        # Validasi keberadaan kolom
        missing_headers = [h for h in self.header_names if h not in df.columns]
        if missing_headers:
            raise ValueError(f"Header tidak ditemukan dalam DataFrame: {', '.join(missing_headers)}")
        
        result_df = df.copy()
        
        # Buat mask untuk setiap kolom (True = tidak kosong, False = kosong)
        mask = pd.Series(True, index=df.index)
        for header in self.header_names:
            # Kondisi not kosong: tidak NA dan isinya tidak string kosong
            col_mask = df[header].notna() & (df[header].astype(str).str.strip() != '')
            mask = mask & col_mask
        
        # Filter baris dengan semua kolom yang ditentukan tidak kosong
        result_df = result_df[mask]
        
        # Reset index setelah filtering
        return result_df.reset_index(drop=True)


class SectionExtractor:
    """
    Mengekstrak section header ke kolom terpisah dan mengkategorikan baris.
    """
    
    def __init__(
        self,
        section_indicator_col: str,
        target_section_col: str,
        remove_section_rows: bool = True
    ):
        self.section_indicator_col = section_indicator_col
        self.target_section_col = target_section_col
        self.remove_section_rows = remove_section_rows
    
    def extract(self, df: pd.DataFrame) -> pd.DataFrame:
        """Mengekstrak section dari DataFrame."""
        if self.section_indicator_col not in df.columns:
            raise ValueError(f"Kolom '{self.section_indicator_col}' tidak ditemukan dalam DataFrame")
        
        result_df = df.copy()
        
        # Tambahkan kolom section baru
        result_df[self.target_section_col] = None
        
        # Dapatkan indeks kolom indikator
        indicator_idx = df.columns.get_loc(self.section_indicator_col)
        
        current_section = None
        section_rows = []
        
        # Identifikasi section dan isi kolom target
        for idx, row in result_df.iterrows():
            # Cek apakah row ini adalah section header:
            # 1. Nilai di kolom indikator tidak kosong
            # 2. Cell di sekitarnya (kiri dan kanan) kosong
            value = row[self.section_indicator_col]
            
            # Cek jika nilai di kolom indikator tidak kosong
            is_value_present = isinstance(value, str) and value.strip() != "" or (not pd.isna(value) and value is not None)
            
            # Cek jika cell di sekitarnya kosong (kiri dan kanan)
            has_empty_surroundings = True
            
            # Periksa cell di kiri (jika bukan kolom pertama)
            if indicator_idx > 0:
                left_value = row.iloc[indicator_idx - 1]
                if isinstance(left_value, str) and left_value.strip() != "" or (not pd.isna(left_value) and left_value is not None):
                    has_empty_surroundings = False
            
            # Periksa cell di kanan (jika bukan kolom terakhir)
            if indicator_idx < len(row) - 1:
                right_value = row.iloc[indicator_idx + 1]
                if isinstance(right_value, str) and right_value.strip() != "" or (not pd.isna(right_value) and right_value is not None):
                    has_empty_surroundings = False
            
            # Pastikan sebagian besar cell di baris ini kosong
            empty_cell_count = sum(1 for x in row if pd.isna(x) or (isinstance(x, str) and not x.strip()))
            mostly_empty = empty_cell_count >= len(row) * 0.7  # 70% cell kosong
            
            is_section = is_value_present and has_empty_surroundings and mostly_empty
            
            if is_section:
                current_section = value
                section_rows.append(idx)
            
            result_df.at[idx, self.target_section_col] = current_section
        
        # Hapus baris section jika diperlukan
        if self.remove_section_rows and section_rows:
            result_df = result_df.drop(section_rows)
        
        # Reset index setelah perubahan
        return result_df.reset_index(drop=True)


class FieldMapper:
    """
    Memetakan kolom sumber ke field target dengan nama baru.
    """
    
    def __init__(
        self,
        mapping: Dict[str, str],
        transform_functions: Optional[Dict[str, Callable]] = None,
        default_values: Optional[Dict[str, Any]] = None
    ):
        """
        Inisialisasi dengan mapping kolom sumber ke kolom target.
        
        Args:
            mapping: Dictionary mapping dari {kolom_sumber: kolom_target}
            transform_functions: Dictionary {kolom_target: fungsi_transformasi}
            default_values: Dictionary {kolom_target: nilai_default}
        """
        self.mapping = mapping  # {source_col: target_col}
        self.transform_functions = transform_functions or {}
        self.default_values = default_values or {}
    
    def map_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Memetakan kolom dari DataFrame sumber ke DataFrame target dengan nama baru.
        
        Contoh:
            Jika dataframe input punya kolom ['description', 'quantity', 'supplier']
            Dan mapping = {'description': 'Name', 'quantity': 'Jumlah', 'supplier': 'Vendor'}
            
            Maka output DataFrame akan punya kolom ['Name', 'Jumlah', 'Vendor'] dengan
            data yang sesuai dari dataframe input.
        """
        # Buat DataFrame baru dengan kolom target
        result_df = pd.DataFrame()
        
        # Salin data dari kolom sumber ke kolom target
        for source_col, target_col in self.mapping.items():
            if source_col in df.columns:
                # Salin data dari kolom sumber ke kolom target dengan nama baru
                result_df[target_col] = df[source_col].copy()
            elif target_col in self.default_values:
                # Jika kolom sumber tidak ada tapi ada nilai default untuk target
                result_df[target_col] = self.default_values[target_col]
            else:
                # Jika kolom sumber tidak ada dan tidak ada nilai default, isi dengan NaN
                result_df[target_col] = np.nan
        
        # Terapkan fungsi transformasi jika ada
        for target_col, transform_func in self.transform_functions.items():
            if target_col in result_df.columns:
                result_df[target_col] = result_df[target_col].apply(transform_func)
        
        # Tambahkan field default yang belum ada di result_df
        for target_col, default_value in self.default_values.items():
            if target_col not in result_df.columns:
                result_df[target_col] = default_value
        
        return result_df


class DataFrameJoiner:
    """
    Menggabungkan dua DataFrame berdasarkan kolom kunci yang sama/terkait.
    """
    
    def __init__(
        self,
        left_key: str,
        right_key: str,
        join_type: str = "left",
        columns_to_add: Optional[List[str]] = None,
        target_column_names: Optional[Dict[str, str]] = None,
        match_case: bool = False
    ):
        self.left_key = left_key
        self.right_key = right_key
        self.join_type = join_type
        self.columns_to_add = columns_to_add
        self.target_column_names = target_column_names or {}
        self.match_case = match_case
    
    def join(self, left_df: pd.DataFrame, right_df: pd.DataFrame) -> pd.DataFrame:
        """Menggabungkan dua DataFrame berdasarkan kunci."""
        # Validasi keberadaan kolom kunci
        if self.left_key not in left_df.columns:
            raise ValueError(f"Kolom '{self.left_key}' tidak ditemukan dalam DataFrame kiri")
        if self.right_key not in right_df.columns:
            raise ValueError(f"Kolom '{self.right_key}' tidak ditemukan dalam DataFrame kanan")
        
        # Pilih kolom yang akan ditambahkan dari right_df
        if self.columns_to_add:
            # Validasi kolom yang akan ditambahkan
            invalid_cols = [col for col in self.columns_to_add if col not in right_df.columns]
            if invalid_cols:
                raise ValueError(f"Kolom tidak ditemukan di DataFrame kanan: {', '.join(invalid_cols)}")
            
            columns_to_use = [self.right_key] + self.columns_to_add
            right_df_subset = right_df[columns_to_use].copy()
        else:
            # Gunakan semua kolom selain right_key
            right_df_subset = right_df.copy()
        
        # Rename kolom sesuai target_column_names
        for old_name, new_name in self.target_column_names.items():
            if old_name in right_df_subset.columns:
                right_df_subset = right_df_subset.rename(columns={old_name: new_name})
        
        # Persiapkan data untuk join jika tidak match_case
        left_df_copy = left_df.copy()
        right_df_copy = right_df_subset.copy()
        
        if not self.match_case:
            # Konversi kunci ke lowercase untuk non-case-sensitive matching
            left_df_copy[f'__{self.left_key}_lower'] = left_df_copy[self.left_key].astype(str).str.lower()
            right_df_copy[f'__{self.right_key}_lower'] = right_df_copy[self.right_key].astype(str).str.lower()
            
            join_key_left = f'__{self.left_key}_lower'
            join_key_right = f'__{self.right_key}_lower'
        else:
            join_key_left = self.left_key
            join_key_right = self.right_key
        
        # Lakukan join
        result_df = pd.merge(
            left_df_copy,
            right_df_copy,
            how=self.join_type,
            left_on=join_key_left,
            right_on=join_key_right,
            suffixes=('', '_right')
        )
        
        # Hapus kolom temporary jika menggunakan non-case-sensitive matching
        if not self.match_case:
            result_df = result_df.drop(columns=[f'__{self.left_key}_lower', f'__{self.right_key}_lower'])
        
        # Hapus kolom duplikat dari right_df (biasanya kolom kunci)
        if f'{self.right_key}_right' in result_df.columns:
            result_df = result_df.drop(columns=[f'{self.right_key}_right'])
        elif self.right_key != self.left_key and self.right_key in result_df.columns:
            result_df = result_df.drop(columns=[self.right_key])
        
        return result_df

class DuplicateSuppressor:
    """
    Menghilangkan tampilan nilai duplikat pada dataframe untuk meningkatkan keterbacaan.
    
    Transformasi ini akan mengosongkan nilai-nilai yang berulang pada kolom tertentu
    untuk baris-baris berurutan, contoh:
    
    Sebelum:
    name | age | nationality | skills | level
    alex | 23  | american    | sword  | intermediate
    alex | 23  | american    | judo   | beginner
    alex | 23  | american    | knit   | master
    
    Sesudah:
    name | age | nationality | skills | level
    alex | 23  | american    | sword  | intermediate
         |     |             | judo   | beginner
         |     |             | knit   | master
    """
    
    def __init__(
        self,
        columns_to_suppress: Union[str, List[str]],
        sort_data: bool = True,
        group_by: Optional[Union[str, List[str]]] = None,
        replacement_value: Any = ""
    ):
        """
        Inisialisasi DuplicateSuppressor.
        
        Args:
            columns_to_suppress: Kolom atau list kolom yang akan dikosongkan jika terdapat duplikat
            sort_data: Boolean untuk mengurutkan data berdasarkan kolom grup (default: True)
            group_by: Kolom untuk mengelompokkan data. Jika None, menggunakan columns_to_suppress
            replacement_value: Nilai yang digunakan untuk menggantikan nilai duplikat (default: "")
        """
        if isinstance(columns_to_suppress, str):
            self.columns_to_suppress = [columns_to_suppress]
        else:
            self.columns_to_suppress = columns_to_suppress
            
        self.sort_data = sort_data
        
        # Jika group_by tidak ditentukan, gunakan columns_to_suppress
        if group_by is None:
            self.group_by = self.columns_to_suppress.copy()
        elif isinstance(group_by, str):
            self.group_by = [group_by]
        else:
            self.group_by = group_by
            
        self.replacement_value = replacement_value
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform DataFrame dengan menghilangkan tampilan nilai duplikat.
        
        Args:
            df: DataFrame untuk ditransformasi
            
        Returns:
            DataFrame dengan tampilan nilai duplikat dihilangkan
        """
        # Validasi keberadaan kolom
        missing_cols = [col for col in self.columns_to_suppress if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Kolom tidak ditemukan dalam DataFrame: {', '.join(missing_cols)}")
            
        missing_group_cols = [col for col in self.group_by if col not in df.columns]
        if missing_group_cols:
            raise ValueError(f"Kolom grouping tidak ditemukan dalam DataFrame: {', '.join(missing_group_cols)}")
        
        # Jika DataFrame kosong atau hanya memiliki satu baris, return as is
        if len(df) <= 1:
            return df.copy()
        
        # Salin DataFrame untuk hasil
        result_df = df.copy()
        
        # Urutkan data berdasarkan kolom grouping jika diminta
        if self.sort_data:
            result_df = result_df.sort_values(by=self.group_by).reset_index(drop=True)
        
        # Simpan tipe data asli untuk kolom yang akan di-suppress
        original_dtypes = {col: result_df[col].dtype for col in self.columns_to_suppress}
        
        # Iterate through DataFrame
        for idx in range(1, len(result_df)):  # Start from second row
            # Cek apakah baris ini dalam grup yang sama dengan baris sebelumnya
            same_group = True
            for col in self.group_by:
                current_val = result_df.iloc[idx][col]
                prev_val = result_df.iloc[idx-1][col]
                
                # Handle NaN values
                if pd.isna(current_val) and pd.isna(prev_val):
                    continue  # Both NaN, considered equal
                elif pd.isna(current_val) or pd.isna(prev_val):
                    same_group = False  # One is NaN, one is not
                    break
                elif current_val != prev_val:
                    same_group = False
                    break
            
            # Jika dalam grup yang sama, cek nilai kolom yang akan di-suppress
            if same_group:
                for col in self.columns_to_suppress:
                    current_val = result_df.iloc[idx][col]
                    prev_val = result_df.iloc[idx-1][col]
                    
                    # Handle NaN values
                    if pd.isna(current_val) and pd.isna(prev_val):
                        continue  # Both NaN, considered equal
                    elif pd.isna(current_val) or pd.isna(prev_val):
                        continue  # One is NaN, different values
                    elif current_val != prev_val:
                        continue  # Different values
                    
                    # Jika sama, suppress dengan replacement_value
                    result_df.iloc[idx, result_df.columns.get_loc(col)] = self.replacement_value
        
        # Kembalikan tipe data asli jika replacement_value adalah string kosong
        if self.replacement_value == "":
            for col, dtype in original_dtypes.items():
                # Jika kolom asli adalah numerik dan kita menggantinya dengan string kosong,
                # akan menyebabkan error. Maka kita konversi kembali ke dtype asli dengan
                # penanganan khusus untuk NaN
                if pd.api.types.is_numeric_dtype(dtype):
                    # Ganti string kosong dengan NaN untuk kolom numerik
                    mask = result_df[col] == ""
                    if mask.any():
                        result_df.loc[mask, col] = np.nan
                    
                # Coba konversi kembali ke tipe data asli
                try:
                    result_df[col] = result_df[col].astype(dtype)
                except:
                    pass  # Jika konversi gagal, biarkan tipe data saat ini
        
        return result_df

class DuplicateRestorer:
    """
    Mengisi kembali nilai yang kosong dengan nilai terakhir yang tidak kosong.
    
    Transformasi ini adalah kebalikan dari DuplicateSuppressor, mengisi nilai kosong
    dengan nilai terakhir yang valid dari kolom yang sama, contoh:
    
    Sebelum:
    name | age | nationality | skills | level
    alex | 23  | american    | sword  | intermediate
         |     |             | judo   | beginner
         |     |             | knit   | master
    
    Sesudah:
    name | age | nationality | skills | level
    alex | 23  | american    | sword  | intermediate
    alex | 23  | american    | judo   | beginner
    alex | 23  | american    | knit   | master
    """
    
    def __init__(
        self,
        columns_to_restore: Union[str, List[str]],
        sort_data: bool = True,
        group_by: Optional[Union[str, List[str]]] = None,
        consider_empty_as_null: bool = True
    ):
        """
        Inisialisasi DuplicateRestorer.
        
        Args:
            columns_to_restore: Kolom atau list kolom yang akan diisi dengan nilai terakhir yang tidak kosong
            sort_data: Boolean untuk mengurutkan data berdasarkan kolom grup (default: True)
            group_by: Kolom untuk mengelompokkan data. Jika None, akan membuat grup baru setiap nilai non-null ditemukan
            consider_empty_as_null: Boolean untuk memperlakukan string kosong sebagai null (default: True)
        """
        if isinstance(columns_to_restore, str):
            self.columns_to_restore = [columns_to_restore]
        else:
            self.columns_to_restore = columns_to_restore
            
        self.sort_data = sort_data
        
        # Konfigurasi grup
        if group_by is None:
            self.group_by = None
        elif isinstance(group_by, str):
            self.group_by = [group_by]
        else:
            self.group_by = group_by
            
        self.consider_empty_as_null = consider_empty_as_null
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform DataFrame dengan mengisi nilai kosong dengan nilai terakhir yang tidak kosong.
        
        Args:
            df: DataFrame untuk ditransformasi
            
        Returns:
            DataFrame dengan nilai kosong diisi kembali
        """
        # Validasi keberadaan kolom
        missing_cols = [col for col in self.columns_to_restore if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Kolom tidak ditemukan dalam DataFrame: {', '.join(missing_cols)}")
            
        if self.group_by:
            missing_group_cols = [col for col in self.group_by if col not in df.columns]
            if missing_group_cols:
                raise ValueError(f"Kolom grouping tidak ditemukan dalam DataFrame: {', '.join(missing_group_cols)}")
        
        # Jika DataFrame kosong, return as is
        if len(df) == 0:
            return df.copy()
        
        # Salin DataFrame untuk hasil
        result_df = df.copy()
        
        # Urutkan data berdasarkan kolom grouping jika diminta
        if self.sort_data and self.group_by:
            result_df = result_df.sort_values(by=self.group_by).reset_index(drop=True)
        
        # Fungsi untuk menentukan apakah nilai dianggap kosong
        def is_empty(val):
            if pd.isna(val):
                return True
            if self.consider_empty_as_null and isinstance(val, str) and val.strip() == "":
                return True
            return False
        
        if self.group_by:
            # Gunakan groupby untuk memproses setiap grup secara terpisah
            # Ini menangani kasus data yang mungkin tidak berurutan
            groups = result_df.groupby(self.group_by)
            
            for group_name, group_df in groups:
                group_indices = group_df.index
                
                # Iterate kolom yang akan diisi
                for col in self.columns_to_restore:
                    # Track nilai terakhir yang valid untuk kolom ini dalam grup
                    last_value = None
                    first_valid_idx = None
                    
                    # Cari nilai pertama yang valid dalam grup untuk kolom ini
                    for idx in group_indices:
                        val = result_df.at[idx, col]
                        if not is_empty(val):
                            first_valid_idx = idx
                            last_value = val
                            break
                    
                    # Jika tidak ada nilai valid dalam grup, lanjutkan ke kolom berikutnya
                    if last_value is None:
                        continue
                    
                    # Dalam urutan maju, isi nilai kosong
                    for idx in group_indices:
                        val = result_df.at[idx, col]
                        if is_empty(val):
                            result_df.at[idx, col] = last_value
                        else:
                            # Update last value
                            last_value = val
        else:
            # Tanpa grup, restore berdasarkan urutan baris dan perubahan nilai
            # Iterate kolom yang akan diisi
            for col in self.columns_to_restore:
                # Cari nilai non-null pertama di kolom
                last_value = None
                
                # Catat indeks dimana terjadi perubahan nilai (bukan null-ke-nilai)
                change_indices = []
                prev_non_null = None
                
                for idx in range(len(result_df)):
                    val = result_df.iloc[idx][col]
                    
                    if not is_empty(val):
                        if last_value is None:
                            last_value = val
                        
                        # Jika nilai berbeda dengan nilai non-null sebelumnya, catat perubahan
                        if prev_non_null is not None and val != prev_non_null:
                            change_indices.append(idx)
                        
                        prev_non_null = val
                
                # Jika tidak ditemukan nilai valid, lewati kolom ini
                if last_value is None:
                    continue
                
                # Tambahkan indeks 0 dan len(df) ke daftar indeks perubahan
                if 0 not in change_indices:
                    change_indices.insert(0, 0)
                change_indices.append(len(result_df))
                
                # Proses setiap segmen terpisah (segmen = rentang antara dua perubahan nilai)
                for i in range(len(change_indices) - 1):
                    start_idx = change_indices[i]
                    end_idx = change_indices[i+1]
                    
                    # Cari nilai non-null pertama di segmen ini
                    first_value = None
                    for idx in range(start_idx, end_idx):
                        val = result_df.iloc[idx][col]
                        if not is_empty(val):
                            first_value = val
                            break
                    
                    # Jika tidak ada nilai non-null di segmen ini, gunakan nilai dari segmen sebelumnya
                    if first_value is None:
                        continue
                    
                    # Isi nilai kosong di segmen ini dengan nilai first_value
                    for idx in range(start_idx, end_idx):
                        val = result_df.iloc[idx][col]
                        if is_empty(val):
                            result_df.iloc[idx, result_df.columns.get_loc(col)] = first_value
        
        return result_df

class TextPrefixFormatter:
    """
    Memastikan semua nilai di kolom tertentu memiliki awalan teks yang seragam.
    
    Transformasi ini memeriksa nilai di kolom target, dan menambahkan awalan teks
    jika belum ada. Jika nilai sudah memiliki awalan teks tersebut, nilai dibiarkan
    seperti apa adanya untuk menghindari duplikasi.
    
    Contoh:
    Dengan prefix "R - " pada kolom "skills":
    
    Sebelum:
    name | age | nationality | skills      | level
    alex | 23  | american    | R - sword   | intermediate
    alex | 23  | american    | judo        | beginner
    alex | 23  | american    | knit        | master
    
    Sesudah:
    name | age | nationality | skills      | level
    alex | 23  | american    | R - sword   | intermediate
    alex | 23  | american    | R - judo    | beginner
    alex | 23  | american    | R - knit    | master
    """
    
    def __init__(
        self,
        column: str,
        prefix: str,
        skip_na: bool = True
    ):
        """
        Inisialisasi TextPrefixFormatter.
        
        Args:
            column: Kolom yang akan diformat
            prefix: Awalan teks yang akan ditambahkan jika belum ada
            skip_na: Boolean untuk melewati nilai NA (tidak menambahkan prefix)
        """
        self.column = column
        self.prefix = prefix
        self.skip_na = skip_na
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform DataFrame dengan memastikan semua nilai di kolom target memiliki awalan teks.
        
        Args:
            df: DataFrame untuk ditransformasi
            
        Returns:
            DataFrame dengan nilai di kolom target yang sudah diformat
        """
        # Validasi keberadaan kolom
        if self.column not in df.columns:
            raise ValueError(f"Kolom '{self.column}' tidak ditemukan dalam DataFrame")
        
        # Salin DataFrame untuk hasil
        result_df = df.copy()
        
        # Fungsi untuk menambahkan prefix jika belum ada
        def add_prefix_if_missing(text):
            if pd.isna(text):
                # Jika nilai adalah NA dan skip_na=True, return as is
                if self.skip_na:
                    return text
                # Jika tidak, tambahkan prefix ke string kosong
                else:
                    return self.prefix
            
            # Konversi ke string untuk memastikan
            text_str = str(text).strip()
            
            # Cek apakah sudah memiliki prefix
            if text_str.startswith(self.prefix):
                return text_str
            else:
                return self.prefix + text_str
        
        # Terapkan fungsi ke kolom target
        result_df[self.column] = result_df[self.column].apply(add_prefix_if_missing)
        
        return result_df
    
class StaticFieldAdder:
    """
    Menambahkan kolom baru dengan nilai statis ke DataFrame.
    
    Transformasi ini menambahkan satu atau beberapa kolom baru ke DataFrame
    dengan nilai yang sama untuk semua baris.
    
    Contoh:
    Menambahkan kolom "number" dengan nilai 3:
    
    Sebelum:
    name | age | nationality | skills      | level
    alex | 23  | american    | sword       | intermediate
    alex | 23  | american    | judo        | beginner
    
    Sesudah:
    name | age | nationality | skills      | level        | number
    alex | 23  | american    | sword       | intermediate | 3
    alex | 23  | american    | judo        | beginner     | 3
    """
    
    def __init__(
        self,
        fields_to_add: Dict[str, Any]
    ):
        """
        Inisialisasi StaticFieldAdder.
        
        Args:
            fields_to_add: Dictionary dengan format {nama_kolom: nilai_statis}
        """
        self.fields_to_add = fields_to_add
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform DataFrame dengan menambahkan kolom baru dengan nilai statis.
        
        Args:
            df: DataFrame untuk ditransformasi
            
        Returns:
            DataFrame dengan kolom baru yang ditambahkan
        """
        # Salin DataFrame untuk hasil
        result_df = df.copy()
        
        # Tambahkan setiap kolom baru
        for field_name, field_value in self.fields_to_add.items():
            # Periksa apakah kolom sudah ada
            if field_name in result_df.columns:
                raise ValueError(f"Kolom '{field_name}' sudah ada dalam DataFrame")
            
            # Tambahkan kolom baru dengan nilai statis
            result_df[field_name] = field_value
        
        return result_df
    

class ColumnReorderer:

    """
    Mengubah urutan kolom DataFrame sesuai dengan urutan yang diinginkan.
    
    Transformasi ini mengatur ulang urutan kolom tanpa mengubah data.
    Kolom yang tidak disebutkan dalam urutan baru akan ditambahkan di akhir
    dengan urutan aslinya dipertahankan.
    
    Contoh:
    Mengubah urutan dari "name,age,nationality,skills,level"
    menjadi "age,name,nationality,number,skills,level":
    
    Sebelum:
    name | age | nationality | skills      | level        | number
    alex | 23  | american    | sword       | intermediate | 3
    
    Sesudah:
    age  | name | nationality | number | skills      | level
    23   | alex | american    | 3      | sword       | intermediate
    """
    
    def __init__(
        self,
        column_order: List[str],
        include_remaining: bool = True
    ):
        """
        Inisialisasi ColumnReorderer.
        
        Args:
            column_order: List urutan kolom yang diinginkan
            include_remaining: Boolean untuk menyertakan kolom yang tidak disebutkan di akhir
        """
        self.column_order = column_order
        self.include_remaining = include_remaining
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform DataFrame dengan mengubah urutan kolom.
        
        Args:
            df: DataFrame untuk ditransformasi
            
        Returns:
            DataFrame dengan urutan kolom yang diubah
        """
        # Validasi keberadaan kolom
        missing_cols = [col for col in self.column_order if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Kolom tidak ditemukan dalam DataFrame: {', '.join(missing_cols)}")
        
        # Siapkan urutan kolom baru
        if self.include_remaining:
            # Tambahkan kolom yang tidak disebutkan di akhir
            remaining_cols = [col for col in df.columns if col not in self.column_order]
            new_column_order = self.column_order + remaining_cols
        else:
            # Hanya gunakan kolom yang disebutkan
            new_column_order = self.column_order
        
        # Reorder kolom
        result_df = df[new_column_order].copy()
        
        return result_df
    

class DataFrameSorter:
    """
    Komponen untuk mengurutkan DataFrame berdasarkan satu atau beberapa kolom
    dengan berbagai opsi kontrol.
    
    Transformasi ini menyediakan antarmuka yang fleksibel untuk mengurutkan
    data dengan mendukung pengurutan ascending/descending, pengurutan multi-kolom,
    pengurutan kustom, dan penanganan nilai null.
    
    Contoh:
    Mengurutkan data berdasarkan 'name' (ascending) kemudian 'age' (descending):
    
    Sebelum:
    name  | age | skills    | level
    adi   | 23  | aikido    | beginner
    bernard | 14 | drawing   | intermediate
    adi   | 23  | swimming  | beginner
    
    Sesudah:
    name  | age | skills    | level
    adi   | 23  | aikido    | beginner
    adi   | 23  | swimming  | beginner
    bernard | 14 | drawing   | intermediate
    """
    
    def __init__(
        self,
        sort_columns: Union[str, List[str], Dict[str, bool]],
        ascending: Union[bool, List[bool]] = True,
        na_position: str = 'last',
        reset_index: bool = True,
        custom_order: Optional[Dict[str, List[Any]]] = None
    ):
        """
        Inisialisasi DataFrameSorter.
        
        Args:
            sort_columns: Kolom untuk pengurutan. Bisa berupa:
                          - String untuk satu kolom
                          - List untuk beberapa kolom
                          - Dict dengan format {kolom: ascending} untuk menentukan arah pengurutan per kolom
            ascending: Boolean atau list boolean untuk menentukan arah pengurutan (True=ascending, False=descending)
                       Jika list, harus memiliki panjang yang sama dengan sort_columns
            na_position: Posisi nilai null ('first' atau 'last')
            reset_index: Boolean untuk reset index setelah pengurutan
            custom_order: Dictionary dengan format {kolom: [daftar_nilai]} untuk pengurutan kustom
                         Nilai akan diurutkan sesuai urutannya dalam daftar
        """
        # Normalisasi parameter sort_columns
        if isinstance(sort_columns, str):
            self.sort_columns = [sort_columns]
            self.ascending_values = [ascending] if isinstance(ascending, bool) else ascending
        elif isinstance(sort_columns, list):
            self.sort_columns = sort_columns
            if isinstance(ascending, bool):
                self.ascending_values = [ascending] * len(sort_columns)
            else:
                self.ascending_values = ascending
                if len(ascending) != len(sort_columns):
                    raise ValueError("Jika ascending adalah list, panjangnya harus sama dengan sort_columns")
        elif isinstance(sort_columns, dict):
            self.sort_columns = list(sort_columns.keys())
            self.ascending_values = list(sort_columns.values())
        else:
            raise ValueError("sort_columns harus berupa str, list, atau dict")
        
        self.na_position = na_position
        if na_position not in ['first', 'last']:
            raise ValueError("na_position harus 'first' atau 'last'")
        
        self.reset_index = reset_index
        self.custom_order = custom_order or {}
        
        # Validasi custom_order
        for col, order_list in self.custom_order.items():
            if not isinstance(order_list, list):
                raise ValueError(f"Custom order untuk kolom {col} harus berupa list")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform DataFrame dengan mengurutkan berdasarkan kriteria yang ditentukan.
        
        Args:
            df: DataFrame untuk ditransformasi
            
        Returns:
            DataFrame yang sudah diurutkan
        """
        # Validasi keberadaan kolom
        missing_cols = [col for col in self.sort_columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Kolom tidak ditemukan dalam DataFrame: {', '.join(missing_cols)}")
        
        # Validasi custom_order columns
        missing_custom_cols = [col for col in self.custom_order.keys() if col not in df.columns]
        if missing_custom_cols:
            raise ValueError(f"Kolom custom order tidak ditemukan dalam DataFrame: {', '.join(missing_custom_cols)}")
        
        # Salin DataFrame untuk hasil
        result_df = df.copy()
        
        # Terapkan custom ordering jika ada
        if self.custom_order:
            for col, order_list in self.custom_order.items():
                # Buat kategorikal berdasarkan custom order
                cat_type = pd.CategoricalDtype(categories=order_list, ordered=True)
                result_df[col] = result_df[col].astype(cat_type)
        
        # Lakukan pengurutan
        result_df = result_df.sort_values(
            by=self.sort_columns,
            ascending=self.ascending_values,
            na_position=self.na_position
        )
        
        # Reset index jika diminta
        if self.reset_index:
            result_df = result_df.reset_index(drop=True)
        
        # Kembalikan tipe data asli untuk kolom yang menggunakan custom ordering
        for col in self.custom_order.keys():
            # Kembalikan ke tipe data asli (atau object sebagai fallback)
            result_df[col] = result_df[col].astype(df[col].dtype)
        
        return result_df
    
    @classmethod
    def sort_by_multiple(
        cls,
        df: pd.DataFrame,
        sort_spec: Dict[str, Union[bool, Dict[str, Any]]],
        reset_index: bool = True
    ) -> pd.DataFrame:
        """
        Metode utilitas untuk mengurutkan DataFrame dengan spesifikasi yang lebih kompleks.
        
        Args:
            df: DataFrame untuk diurutkan
            sort_spec: Dictionary dengan format 
                       {kolom: True/False} untuk ascending/descending sederhana, atau
                       {kolom: {'ascending': True/False, 'na_position': 'first'/'last', 
                                'custom_order': [val1, val2,...]}} untuk pengaturan detail
            reset_index: Boolean untuk reset index setelah pengurutan
            
        Returns:
            DataFrame yang sudah diurutkan
        """
        sort_columns = []
        ascending_values = []
        custom_order = {}
        na_position = 'last'  # Default
        
        for col, spec in sort_spec.items():
            sort_columns.append(col)
            
            if isinstance(spec, bool):
                # Format sederhana {kolom: True/False}
                ascending_values.append(spec)
            elif isinstance(spec, dict):
                # Format detail dengan opsi tambahan
                ascending_values.append(spec.get('ascending', True))
                
                # Custom order untuk kolom ini
                if 'custom_order' in spec:
                    custom_order[col] = spec['custom_order']
                    
                # Gunakan na_position yang terakhir dilihat
                if 'na_position' in spec:
                    na_position = spec['na_position']
            else:
                raise ValueError(f"Spesifikasi tidak valid untuk kolom {col}")
        
        # Buat instance sorter dan transform
        sorter = cls(
            sort_columns=sort_columns,
            ascending=ascending_values,
            na_position=na_position,
            reset_index=reset_index,
            custom_order=custom_order
        )
        
        return sorter.transform(df)