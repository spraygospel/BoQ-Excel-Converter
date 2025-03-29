#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import xmlrpc.client
import pandas as pd
import sys
import argparse
import os
import json
import re
from tabulate import tabulate

class OdooExcelUploader:
    """
    Script untuk upload data Excel ke Odoo
    """
    
    def __init__(self, url, db, username, password):
        """Inisialisasi koneksi ke Odoo"""
        self.url = url
        self.db = db
        self.username = username
        self.password = password
        
        # XML-RPC endpoints
        self.common_endpoint = f'{url}/xmlrpc/2/common'
        self.object_endpoint = f'{url}/xmlrpc/2/object'
        self.uid = None
        
        self.connect()
    
    def connect(self):
        """Membuat koneksi dan autentikasi dengan server Odoo"""
        print(f"Menghubungkan ke Odoo di {self.url}...")
        
        try:
            common = xmlrpc.client.ServerProxy(self.common_endpoint)
            self.uid = common.authenticate(self.db, self.username, self.password, {})
            
            if not self.uid:
                print("Autentikasi gagal. Periksa kredensial.")
                sys.exit(1)
                
            print(f"Terhubung sebagai {self.username} (uid: {self.uid})")
            
            # Initialize models object
            self.models = xmlrpc.client.ServerProxy(self.object_endpoint)
            
        except Exception as e:
            print(f"Koneksi gagal: {str(e)}")
            sys.exit(1)
    
    def get_model_fields(self, model_name):
        """Mendapatkan daftar field dan labelnya untuk model tertentu"""
        print(f"Mengambil fields untuk model: {model_name}")
        
        try:
            fields = self.models.execute_kw(
                self.db, self.uid, self.password,
                model_name, 'fields_get',
                [],
                {'attributes': ['string', 'help', 'type', 'required', 'readonly', 'selection', 'relation']}
            )
            
            return fields
            
        except Exception as e:
            print(f"Error mendapatkan fields: {str(e)}")
            return {}
    
    def display_model_fields(self, model_name):
        """Menampilkan daftar field dari model"""
        fields = self.get_model_fields(model_name)
        
        if not fields:
            print(f"Tidak dapat mendapatkan fields untuk {model_name}")
            return {}, {}
        
        # Membuat list untuk ditampilkan dalam tabel
        field_list = []
        for field_name, field_info in fields.items():
            # Skip fields internal
            if field_name.startswith('_') or field_name in ['create_uid', 'create_date', 'write_uid', 'write_date', '__last_update']:
                continue
                
            field_type = field_info.get('type', '')
            relation = field_info.get('relation', '')
            if relation:
                field_type = f"{field_type} -> {relation}"
            
            required = "✓" if field_info.get('required', False) else ""
            readonly = "✓" if field_info.get('readonly', False) else ""
            
            field_list.append([
                field_name,
                field_info.get('string', ''),
                field_type,
                required,
                readonly
            ])
        
        # Sorting berdasarkan nama field
        field_list.sort(key=lambda x: x[0])
        
        # Tampilkan tabel
        print("\nDaftar fields untuk model", model_name, ":")
        print(tabulate(field_list, headers=["Field Name", "Label", "Type", "Required", "Readonly"]))
        
        # Buat field mapping untuk digunakan nanti
        field_mapping = {}  # field_name -> label
        label_to_field = {}  # label -> field_name
        
        for field_name, field_info in fields.items():
            label = field_info.get('string', '')
            field_mapping[field_name] = label
            # Gunakan versi case-insensitive untuk matching yang lebih baik
            label_to_field[label.lower()] = field_name
        
        return field_mapping, label_to_field
    
    def read_excel(self, excel_file, sheet_name=0):
        """Membaca file Excel"""
        print(f"Membaca file Excel: {excel_file}")
        
        try:
            # Cek ekstensi file
            if not excel_file.endswith(('.xlsx', '.xls')):
                print("WARNING: File tidak berekstensi .xlsx atau .xls. Pastikan ini adalah file Excel.")
            
            # Baca file Excel
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            
            # Bersihkan nama-nama kolom
            df.columns = [str(col).strip() for col in df.columns]
            
            # Drop kolom yang kosong
            df = df.dropna(axis=1, how='all')
            
            # Tampilkan info tentang sheet yang dibaca
            if isinstance(sheet_name, int):
                print(f"Membaca sheet #{sheet_name+1}")
            else:
                print(f"Membaca sheet '{sheet_name}'")
            
            print(f"Jumlah baris: {len(df)}")
            print(f"Jumlah kolom: {len(df.columns)}")
            
            return df
            
        except Exception as e:
            print(f"Error membaca Excel: {str(e)}")
            return None
    
    def list_excel_sheets(self, excel_file):
        """Menampilkan daftar sheet dalam file Excel"""
        print(f"Memeriksa sheet dalam file: {excel_file}")
        
        try:
            # Baca hanya nama sheet
            xlsx = pd.ExcelFile(excel_file)
            sheet_names = xlsx.sheet_names
            
            print(f"File memiliki {len(sheet_names)} sheet:")
            for i, name in enumerate(sheet_names):
                print(f"  {i+1}. {name}")
            
            return sheet_names
            
        except Exception as e:
            print(f"Error memeriksa sheet: {str(e)}")
            return []
    
    def clean_monetary_value(self, value):
        """Membersihkan nilai mata uang menjadi float"""
        if isinstance(value, (int, float)):
            return value
            
        if isinstance(value, str):
            # Hapus simbol mata uang, spasi, tanda kutip
            clean_value = re.sub(r'[Rp$€£¥\s"\']+', '', value)
            # Ganti koma dengan titik jika perlu (untuk format angka)
            clean_value = clean_value.replace(',', '.')
            
            try:
                return float(clean_value)
            except:
                pass
        
        return value
    
    def map_excel_headers_to_fields(self, df, label_to_field):
        """
        Map header Excel (label) ke field name Odoo
        """
        headers = df.columns.tolist()
        mapped_headers = {}
        not_found = []
        
        for header in headers:
            header_key = str(header).lower().strip()
            
            # Coba match langsung
            if header_key in label_to_field:
                mapped_headers[header] = label_to_field[header_key]
                continue
                
            # Coba dengan pencarian partial
            best_match = None
            best_score = 0
            
            for label, field in label_to_field.items():
                # Skip label kosong
                if not label:
                    continue
                    
                # Jika ada kecocokan lengkap
                if header_key == label:
                    best_match = field
                    break
                
                # Jika header ada dalam label atau sebaliknya
                if header_key in label or label in header_key:
                    score = len(set(header_key) & set(label)) / max(len(header_key), len(label))
                    if score > best_score:
                        best_score = score
                        best_match = field
            
            # Minimum score untuk dianggap cocok
            if best_score > 0.5:
                mapped_headers[header] = best_match
            else:
                not_found.append(header)
                
        if not_found:
            print(f"Warning: Beberapa header tidak ditemukan dalam mapping: {not_found}")
        
        # Rename kolom dengan field name
        rename_dict = {h: mapped_headers.get(h, h) for h in headers}
        
        # Tampilkan mapping yang digunakan
        print("\nMapping header Excel ke field Odoo:")
        for original, mapped in rename_dict.items():
            if original in mapped_headers:
                print(f"  {original} -> {mapped}")
        
        # Rename kolom dalam DataFrame
        df_renamed = df.rename(columns=rename_dict)
        
        return df_renamed
    
    def prepare_record_for_api(self, row, model_name):
        """
        Menyiapkan record untuk dikirim ke API
        - Menangani field relasional (many2one, many2many, dll)
        - Membersihkan nilai (currency, boolean, dll)
        """
        record = {}
        fields = self.get_model_fields(model_name)
        
        for field_name, value in row.items():
            # Skip jika nilai kosong atau not a valid field
            if field_name not in fields or pd.isna(value) or value == '':
                continue
                
            # Dapatkan informasi field dari Odoo
            field_info = fields.get(field_name, {})
            field_type = field_info.get('type', 'char')
            
            # Proses nilai sesuai tipe field
            if field_type == 'many2one':
                relation = field_info.get('relation', '')
                if relation and isinstance(value, str):
                    # Coba cari ID berdasarkan nama
                    try:
                        record_ids = self.models.execute_kw(
                            self.db, self.uid, self.password,
                            relation, 'search',
                            [[['name', '=', value]]]
                        )
                        
                        if record_ids:
                            record[field_name] = record_ids[0]
                        else:
                            print(f"Warning: Tidak menemukan {relation} dengan nama '{value}'")
                    except Exception as e:
                        print(f"Error mencari {relation} '{value}': {str(e)}")
                elif isinstance(value, (int, float)) and not pd.isna(value):
                    record[field_name] = int(value)
            
            elif field_type == 'many2many':
                relation = field_info.get('relation', '')
                if relation and isinstance(value, str):
                    # Anggap nilai dipisahkan dengan koma
                    names = [name.strip() for name in value.split(',')]
                    ids = []
                    
                    for name in names:
                        if not name:
                            continue
                        try:
                            record_ids = self.models.execute_kw(
                                self.db, self.uid, self.password,
                                relation, 'search',
                                [[['name', '=', name]]]
                            )
                            
                            if record_ids:
                                ids.append(record_ids[0])
                            else:
                                print(f"Warning: Tidak menemukan {relation} dengan nama '{name}'")
                        except Exception as e:
                            print(f"Error mencari {relation} '{name}': {str(e)}")
                    
                    if ids:
                        record[field_name] = [(6, 0, ids)]
            
            elif field_type == 'boolean':
                # Konversi string ke boolean
                if isinstance(value, str):
                    record[field_name] = value.lower() in ['true', 'yes', 'y', '1', 'ya']
                else:
                    record[field_name] = bool(value)
            
            elif field_type == 'integer':
                try:
                    record[field_name] = int(value)
                except:
                    print(f"Warning: Tidak dapat mengkonversi '{value}' ke integer untuk field {field_name}")
            
            elif field_type == 'float' or field_type == 'monetary':
                # Bersihkan nilai mata uang
                try:
                    clean_value = self.clean_monetary_value(value)
                    if isinstance(clean_value, (int, float)):
                        record[field_name] = clean_value
                except:
                    print(f"Warning: Tidak dapat mengkonversi '{value}' ke float untuk field {field_name}")
            
            elif field_type == 'selection':
                # Pastikan nilai ada dalam opsi selection
                selection_options = field_info.get('selection', [])
                selection_values = [opt[0] for opt in selection_options]
                
                if value in selection_values:
                    record[field_name] = value
                else:
                    # Coba cari berdasarkan label
                    selection_dict = {opt[1].lower(): opt[0] for opt in selection_options}
                    value_lower = str(value).lower()
                    
                    if value_lower in selection_dict:
                        record[field_name] = selection_dict[value_lower]
                    else:
                        print(f"Warning: Nilai '{value}' tidak valid untuk selection field {field_name}")
                        print(f"Opsi yang valid: {[opt[1] for opt in selection_options]}")
            
            else:
                # String dan tipe lainnya
                record[field_name] = str(value) if value is not None else value
        
        # Cek apakah required fields sudah terisi
        required_fields = [name for name, info in fields.items() if info.get('required', False)]
        missing_fields = [name for name in required_fields if name not in record]
        
        if missing_fields:
            print(f"Warning: Field required berikut tidak terisi: {missing_fields}")
            # Coba isi dengan nilai default jika mungkin
            for field_name in missing_fields:
                field_type = fields[field_name].get('type', '')
                if field_type == 'boolean':
                    record[field_name] = False
                elif field_type == 'integer':
                    record[field_name] = 0
                elif field_type == 'float':
                    record[field_name] = 0.0
                elif field_type == 'char':
                    record[field_name] = ''
                elif field_type == 'selection':
                    options = fields[field_name].get('selection', [])
                    if options:
                        record[field_name] = options[0][0]
        
        return record
    
    def post_data_to_odoo(self, df, model_name):
        """Post data dari dataframe ke Odoo via API"""
        print(f"Posting data ke {model_name}")
        
        results = []
        continue_all = False  # Flag untuk skip konfirmasi
        
        # Proses setiap baris
        for i, row in df.iterrows():
            try:
                record = self.prepare_record_for_api(row, model_name)
                
                if not record:
                    print(f"Baris {i+1}: Tidak ada data valid untuk diupload")
                    continue
                
                print(f"Baris {i+1}: Mengirim data: {record}")
                
                # Konfirmasi sebelum POST kecuali sudah pilih "all"
                if not continue_all:
                    confirm = input("Lanjutkan POST data ini? (y/n/all): ").lower()
                    if confirm == 'n':
                        print("POST dibatalkan untuk baris ini.")
                        continue
                    elif confirm == 'all':
                        continue_all = True
                
                # POST data ke Odoo API
                record_id = self.models.execute_kw(
                    self.db, self.uid, self.password,
                    model_name, 'create',
                    [record]
                )
                
                print(f"Baris {i+1}: Berhasil dibuat dengan ID {record_id}")
                results.append({'status': 'success', 'id': record_id, 'row': i+1})
                
            except Exception as e:
                print(f"Baris {i+1}: Error: {str(e)}")
                results.append({'status': 'error', 'error': str(e), 'row': i+1})
        
        # Tampilkan ringkasan hasil
        success = sum(1 for r in results if r['status'] == 'success')
        print(f"\nRingkasan: {success}/{len(df)} baris berhasil diupload")
        
        return results
    
    def upload_excel(self, model_name, excel_file, sheet_name=None):
        """Upload Excel ke model Odoo"""
        # Tampilkan daftar sheet dan pilih jika tidak ditentukan
        sheets = self.list_excel_sheets(excel_file)
        if not sheets:
            print("Error: Tidak dapat membaca sheet dari file Excel")
            return
        
        if sheet_name is None:
            if len(sheets) == 1:
                sheet_name = 0  # Default ke sheet pertama jika hanya ada satu
            else:
                # Minta user pilih sheet
                sheet_choice = input("Pilih nomor sheet yang akan diupload: ")
                try:
                    sheet_idx = int(sheet_choice) - 1
                    if 0 <= sheet_idx < len(sheets):
                        sheet_name = sheet_idx
                    else:
                        print("Nomor sheet tidak valid")
                        return
                except ValueError:
                    # Coba cari berdasarkan nama
                    if sheet_choice in sheets:
                        sheet_name = sheet_choice
                    else:
                        print("Nama sheet tidak valid")
                        return
        
        # Dapatkan dan tampilkan daftar field
        field_mapping, label_to_field = self.display_model_fields(model_name)
        
        # Baca Excel
        df = self.read_excel(excel_file, sheet_name)
        if df is None or df.empty:
            print("Error: Data Excel kosong atau tidak dapat dibaca.")
            return
        
        # Preview data Excel
        print("\nPreview data Excel:")
        print(df.head())
        
        # Map header Excel ke field name
        df_mapped = self.map_excel_headers_to_fields(df, label_to_field)
        
        # Preview data setelah mapping
        print("\nPreview data setelah mapping header:")
        print(df_mapped.head())
        
        # Konfirmasi dari user
        confirm = input("Lanjutkan proses data? (y/n): ")
        if confirm.lower() != 'y':
            print("Proses dibatalkan")
            return
        
        # Tambahan: simpan data ke CSV sementara untuk debugging
        temp_file = "mapped_data.csv"
        df_mapped.to_csv(temp_file, index=False)
        print(f"Data yang sudah dimapping disimpan ke {temp_file}")
        
        # Upload data ke Odoo
        results = self.post_data_to_odoo(df_mapped, model_name)
        
        return results

def main():
    parser = argparse.ArgumentParser(description='Upload data Excel ke Odoo via API')
    
    parser.add_argument('--model', required=True, help='Nama model Odoo (e.g., product.product)')
    parser.add_argument('--excel', required=True, help='Path file Excel (.xlsx/.xls)')
    parser.add_argument('--sheet', help='Nama sheet atau indeks (0-based)')
    ODOO_URL = 'https://api-odoo.visiniaga.com'
    ODOO_DB = 'OdooDev'
    ODOO_USERNAME = 'odoo2@visiniaga.com'
    ODOO_PASSWORD = 'PH8EQ?YF}<ac2A:T9n6%^*'
    args = parser.parse_args()
    
    # Konversi sheet ke int jika berupa angka
    sheet_name = args.sheet
    if sheet_name and sheet_name.isdigit():
        sheet_name = int(sheet_name)
    
    # Upload data
    uploader = OdooExcelUploader(ODOO_URL, ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD)
    uploader.upload_excel(args.model, args.excel, sheet_name)

if __name__ == "__main__":
    main()
