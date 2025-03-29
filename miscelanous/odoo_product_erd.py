#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Odoo Schema Extractor

Script untuk mengekstrak schema database, tabel relasi, dan informasi dari Odoo
yang dibutuhkan untuk memahami struktur data XML ID dan supplier info.

Hasil ekstraksi akan disimpan ke file output 'odoo_schema_info.txt'
"""

import xmlrpc.client
import json
import sys
import os
from datetime import datetime

# Konfigurasi Odoo (sesuaikan dengan konfigurasi Anda)
ODOO_CONFIG = {
    'url': "https://api-odoo.visiniaga.com",
    'db': 'Final6.Copy250301',
    'username': "odoo2@visiniaga.com",
    'password': "PH8EQ?YF}<ac2A:T9n6%^*",
}

def connect_to_odoo():
    """
    Membuat koneksi ke Odoo dan mengembalikan objek yang dibutuhkan
    """
    print(f"[*] Menghubungkan ke Odoo {ODOO_CONFIG['url']}...")
    
    # Buat koneksi ke common endpoint
    common = xmlrpc.client.ServerProxy(f"{ODOO_CONFIG['url']}/xmlrpc/2/common")
    
    # Otentikasi dan dapatkan user ID
    uid = common.authenticate(
        ODOO_CONFIG['db'], 
        ODOO_CONFIG['username'], 
        ODOO_CONFIG['password'], 
        {}
    )
    
    if not uid:
        print("[!] Autentikasi gagal. Periksa kredensial.")
        sys.exit(1)
    
    print(f"[+] Berhasil terhubung sebagai {ODOO_CONFIG['username']} (uid: {uid})")
    
    # Buat koneksi ke object endpoint
    models = xmlrpc.client.ServerProxy(f"{ODOO_CONFIG['url']}/xmlrpc/2/object")
    
    return uid, models

def get_model_fields(uid, models, model_name):
    """
    Mendapatkan informasi field untuk model tertentu
    """
    try:
        fields_data = models.execute_kw(
            ODOO_CONFIG['db'], 
            uid, 
            ODOO_CONFIG['password'],
            model_name, 
            'fields_get', 
            [], 
            {'attributes': ['string', 'help', 'type', 'required', 'relation']}
        )
        return fields_data
    except Exception as e:
        print(f"[!] Error mendapatkan fields untuk {model_name}: {str(e)}")
        return {}

def get_model_record_count(uid, models, model_name):
    """
    Mendapatkan jumlah record untuk model tertentu
    """
    try:
        count = models.execute_kw(
            ODOO_CONFIG['db'], 
            uid, 
            ODOO_CONFIG['password'],
            model_name, 
            'search_count', 
            [[]]
        )
        return count
    except Exception as e:
        print(f"[!] Error mendapatkan jumlah record untuk {model_name}: {str(e)}")
        return 0

def get_xml_id_examples(uid, models, model_name, limit=5):
    """
    Mendapatkan contoh XML ID untuk model tertentu
    """
    try:
        # Cari ID record dari model yang ditentukan
        record_ids = models.execute_kw(
            ODOO_CONFIG['db'], 
            uid, 
            ODOO_CONFIG['password'],
            model_name, 
            'search', 
            [[]], 
            {'limit': limit}
        )
        
        if not record_ids:
            return []
        
        # Dapatkan XML ID dari ir.model.data
        xml_ids = models.execute_kw(
            ODOO_CONFIG['db'],
            uid,
            ODOO_CONFIG['password'],
            'ir.model.data',
            'search_read',
            [[
                ['model', '=', model_name],
                ['res_id', 'in', record_ids]
            ]],
            {'fields': ['name', 'module', 'res_id']}
        )
        
        # Format XML ID dalam bentuk module.name
        formatted_xml_ids = []
        for item in xml_ids:
            formatted_xml_ids.append({
                'xml_id': f"{item['module']}.{item['name']}",
                'res_id': item['res_id']
            })
            
        return formatted_xml_ids
    except Exception as e:
        print(f"[!] Error mendapatkan contoh XML ID untuk {model_name}: {str(e)}")
        return []

def get_specific_xml_id(uid, models, xml_id):
    """
    Mendapatkan informasi tentang record dengan XML ID tertentu
    """
    try:
        # Pisahkan modul dan id
        if '.' in xml_id:
            module, name = xml_id.split('.', 1)
        else:
            return None
        
        # Cari record di ir.model.data
        data = models.execute_kw(
            ODOO_CONFIG['db'],
            uid,
            ODOO_CONFIG['password'],
            'ir.model.data',
            'search_read',
            [[
                ['module', '=', module],
                ['name', '=', name]
            ]],
            {'fields': ['name', 'module', 'model', 'res_id']}
        )
        
        if not data:
            return None
            
        record = data[0]
        model = record['model']
        res_id = record['res_id']
        
        # Dapatkan data record sebenarnya
        record_data = models.execute_kw(
            ODOO_CONFIG['db'],
            uid,
            ODOO_CONFIG['password'],
            model,
            'read',
            [res_id]
        )
        
        if not record_data:
            return None
            
        return {
            'xml_id': xml_id,
            'model': model,
            'res_id': res_id,
            'data': record_data[0]
        }
        
    except Exception as e:
        print(f"[!] Error mendapatkan info untuk XML ID {xml_id}: {str(e)}")
        return None

def get_product_supplier_info(uid, models, product_default_code, supplier_name=None):
    """
    Mendapatkan informasi supplier untuk produk dengan default_code tertentu
    """
    try:
        # Cari produk berdasarkan default_code
        product_ids = models.execute_kw(
            ODOO_CONFIG['db'],
            uid,
            ODOO_CONFIG['password'],
            'product.product',
            'search',
            [[['default_code', '=', product_default_code]]]
        )
        
        if not product_ids:
            return None
            
        product_id = product_ids[0]
        
        # Dapatkan template_id dari produk
        product_data = models.execute_kw(
            ODOO_CONFIG['db'],
            uid,
            ODOO_CONFIG['password'],
            'product.product',
            'read',
            [product_id],
            {'fields': ['product_tmpl_id']}
        )
        
        if not product_data:
            return None
            
        product_tmpl_id = product_data[0]['product_tmpl_id'][0]
        
        # Cari supplier info
        domain = [['product_tmpl_id', '=', product_tmpl_id]]
        
        # Jika supplier_name ditentukan, tambahkan ke domain
        if supplier_name:
            # Cari partner_id berdasarkan nama
            partner_ids = models.execute_kw(
                ODOO_CONFIG['db'],
                uid,
                ODOO_CONFIG['password'],
                'res.partner',
                'search',
                [[['name', 'ilike', supplier_name]]]
            )
            
            if partner_ids:
                domain.append(['name', 'in', partner_ids])
        
        supplier_info = models.execute_kw(
            ODOO_CONFIG['db'],
            uid,
            ODOO_CONFIG['password'],
            'product.supplierinfo',
            'search_read',
            [domain],
            {'fields': ['name', 'price', 'product_code', 'product_name', 'date_start', 'date_end']}
        )
        
        # Dapatkan nama supplier untuk setiap supplier info
        for info in supplier_info:
            if 'name' in info and isinstance(info['name'], list) and len(info['name']) > 0:
                partner_id = info['name'][0]
                partner_data = models.execute_kw(
                    ODOO_CONFIG['db'],
                    uid,
                    ODOO_CONFIG['password'],
                    'res.partner',
                    'read',
                    [partner_id],
                    {'fields': ['name']}
                )
                if partner_data:
                    info['supplier_name'] = partner_data[0]['name']
        
        return {
            'product_id': product_id,
            'product_tmpl_id': product_tmpl_id,
            'supplier_info': supplier_info
        }
        
    except Exception as e:
        print(f"[!] Error mendapatkan supplier info untuk produk {product_default_code}: {str(e)}")
        return None

def extract_schema_info():
    """
    Fungsi utama untuk mengekstrak informasi schema database
    """
    output_file = "odoo_schema_info.txt"
    
    # Connect to Odoo
    uid, models = connect_to_odoo()
    
    # List model yang relevan dengan kebutuhan
    relevant_models = [
        'ir.model.data',         # Model untuk XML ID
        'product.product',        # Model untuk produk
        'product.template',       # Model untuk template produk
        'product.supplierinfo',   # Model untuk informasi supplier produk
        'res.partner'             # Model untuk vendor/supplier
    ]
    
    # Buka file untuk menulis hasil
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"=== ODOO DATABASE SCHEMA INFORMATION ===\n")
        f.write(f"Extracted on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Database: {ODOO_CONFIG['db']}\n")
        f.write(f"URL: {ODOO_CONFIG['url']}\n\n")
        
        # Ekstrak informasi untuk setiap model
        for model_name in relevant_models:
            f.write(f"\n{'='*80}\n")
            f.write(f"MODEL: {model_name}\n")
            f.write(f"{'='*80}\n\n")
            
            # Dapatkan jumlah record
            record_count = get_model_record_count(uid, models, model_name)
            f.write(f"Record Count: {record_count}\n\n")
            
            # Dapatkan informasi field
            fields_data = get_model_fields(uid, models, model_name)
            
            # Tulis informasi field
            f.write("FIELDS:\n")
            f.write("-" * 80 + "\n")
            f.write(f"{'Field Name':<30} {'Type':<15} {'Relation':<30} {'Required':<10}\n")
            f.write("-" * 80 + "\n")
            
            for field_name, field_info in fields_data.items():
                field_type = field_info.get('type', '')
                relation = field_info.get('relation', '')
                required = 'Yes' if field_info.get('required', False) else 'No'
                
                f.write(f"{field_name:<30} {field_type:<15} {relation:<30} {required:<10}\n")
            
            # Dapatkan contoh XML ID
            f.write("\nXML ID EXAMPLES:\n")
            f.write("-" * 80 + "\n")
            
            xml_id_examples = get_xml_id_examples(uid, models, model_name)
            if xml_id_examples:
                for example in xml_id_examples:
                    f.write(f"XML ID: {example['xml_id']}, Res ID: {example['res_id']}\n")
            else:
                f.write("No XML ID examples found.\n")
        
        # Tambahkan informasi khusus untuk XML ID
        f.write("\n\n" + "="*80 + "\n")
        f.write("SPECIAL SECTION: UNDERSTANDING XML IDs\n")
        f.write("="*80 + "\n\n")
        
        f.write("XML IDs dalam Odoo memiliki format 'module.identifier', misalnya 'export.product_product_14828_24ea7638'.\n")
        f.write("- 'export' adalah nama modul\n")
        f.write("- 'product_product_14828_24ea7638' adalah identifier unik untuk record tersebut\n\n")
        
        f.write("XML IDs disimpan dalam tabel ir.model.data dan digunakan untuk mengidentifikasi\n")
        f.write("record secara unik di seluruh database, terutama untuk proses impor/ekspor data.\n\n")
        
        # Cek XML ID yang diberikan user
        xml_ids_to_check = [
            "export.product_product_14828_24ea7638",
            "export.product_supplierinfo_2072_dede4d49"
        ]
        
        f.write("CONTOH ANALISIS XML ID:\n")
        for xml_id in xml_ids_to_check:
            f.write(f"\n--- XML ID: {xml_id} ---\n")
            
            result = get_specific_xml_id(uid, models, xml_id)
            if result:
                f.write(f"Model: {result['model']}\n")
                f.write(f"Database ID: {result['res_id']}\n")
                f.write("Data:\n")
                for key, value in result['data'].items():
                    f.write(f"  {key}: {value}\n")
            else:
                f.write(f"XML ID tidak ditemukan di database.\n")
        
        # Tambahkan informasi khusus untuk product.supplierinfo
        f.write("\n\n" + "="*80 + "\n")
        f.write("SPECIAL SECTION: PRODUCT SUPPLIER INFO RELATIONSHIPS\n")
        f.write("="*80 + "\n\n")
        
        f.write("Hubungan antar model untuk informasi supplier produk:\n\n")
        f.write("1. product.product (memiliki field default_code = 'Internal References')\n")
        f.write("   ↓ (produk memiliki product_tmpl_id yang mengarah ke template)\n")
        f.write("2. product.template (template produk)\n")
        f.write("   ↓ (template memiliki seller_ids yang mengarah ke supplier info)\n")
        f.write("3. product.supplierinfo (info supplier untuk produk)\n")
        f.write("   - Field name: mengarah ke res.partner (vendor)\n")
        f.write("   - Field product_tmpl_id: mengarah ke product.template\n")
        f.write("   - Field product_id: mengarah ke product.product (opsional, untuk variant spesifik)\n")
        f.write("   - Field price: harga dari supplier\n\n")
        
        f.write("CONTOH CARA MENGAMBIL DATA SUPPLIER UNTUK PRODUK:\n\n")
        f.write("1. Cari product.product berdasarkan default_code ('Internal References')\n")
        f.write("2. Dapatkan product_tmpl_id-nya\n")
        f.write("3. Cari di product.supplierinfo berdasarkan product_tmpl_id\n")
        f.write("4. Filter berdasarkan nama supplier jika diperlukan\n\n")
        
        # Tambahkan contoh kode Python untuk mengambil data
        f.write("CONTOH KODE PYTHON:\n\n")
        f.write("```python\n")
        f.write("# Cari produk berdasarkan default_code\n")
        f.write("product_ids = models.execute_kw(\n")
        f.write("    db, uid, password, 'product.product', 'search',\n")
        f.write("    [[['default_code', '=', 'KODE-PRODUK']]]\n")
        f.write(")\n\n")
        f.write("# Dapatkan template_id\n")
        f.write("product_data = models.execute_kw(\n")
        f.write("    db, uid, password, 'product.product', 'read',\n")
        f.write("    [product_ids[0]], {'fields': ['product_tmpl_id']}\n")
        f.write(")\n")
        f.write("product_tmpl_id = product_data[0]['product_tmpl_id'][0]\n\n")
        f.write("# Cari supplier info\n")
        f.write("supplier_info = models.execute_kw(\n")
        f.write("    db, uid, password, 'product.supplierinfo', 'search_read',\n")
        f.write("    [[['product_tmpl_id', '=', product_tmpl_id]]],\n")
        f.write("    {'fields': ['name', 'price']}\n")
        f.write(")\n\n")
        f.write("# Dapatkan nama supplier\n")
        f.write("for info in supplier_info:\n")
        f.write("    partner_id = info['name'][0]\n")
        f.write("    partner_data = models.execute_kw(\n")
        f.write("        db, uid, password, 'res.partner', 'read',\n")
        f.write("        [partner_id], {'fields': ['name']}\n")
        f.write("    )\n")
        f.write("    info['supplier_name'] = partner_data[0]['name']\n")
        f.write("```\n\n")
        
        # Tambahkan contoh data nyata jika ada
        f.write("CONTOH DATA NYATA:\n\n")
        
        # Coba cari produk dengan default_code dari CSV
        example_default_codes = ["C18.18.32", "C18.18.28", "C18.18.22"]
        
        for default_code in example_default_codes:
            f.write(f"Produk dengan default_code = '{default_code}':\n")
            supplier_info = get_product_supplier_info(uid, models, default_code)
            
            if supplier_info:
                f.write(f"  Product ID: {supplier_info['product_id']}\n")
                f.write(f"  Template ID: {supplier_info['product_tmpl_id']}\n")
                
                if supplier_info['supplier_info']:
                    f.write("  Supplier Info:\n")
                    for info in supplier_info['supplier_info']:
                        supplier_name = info.get('supplier_name', info.get('name', 'Unknown'))
                        price = info.get('price', 'N/A')
                        f.write(f"    - Supplier: {supplier_name}, Price: {price}\n")
                else:
                    f.write("  No supplier info found for this product.\n")
            else:
                f.write("  Product not found.\n")
            
            f.write("\n")
    
    print(f"[+] Informasi schema berhasil diekstrak ke file '{output_file}'")
    return output_file

if __name__ == "__main__":
    try:
        output_file = extract_schema_info()
        print(f"[+] Berhasil mengekstrak informasi schema database ke {output_file}")
        print(f"[+] Silakan periksa file tersebut untuk memahami struktur data yang dibutuhkan")
    except Exception as e:
        print(f"[!] Error: {str(e)}")
        sys.exit(1)
