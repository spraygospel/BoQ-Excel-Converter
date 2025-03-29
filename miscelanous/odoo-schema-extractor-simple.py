#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import xmlrpc.client
import json
import sys
import argparse
from pprint import pformat

class OdooSchemaExporter:
    """
    Ekstrak ERD, skema data, dan aturan import dari Odoo untuk digunakan oleh LLM
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
        
        self.models = [
            'product.product',
            'product.template',
            'mrp.bom',
            'mrp.bom.line',
            'sale.order',
            'sale.order.line'
        ]
        
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
            self.models_api = xmlrpc.client.ServerProxy(self.object_endpoint)
            
        except Exception as e:
            print(f"Koneksi gagal: {str(e)}")
            sys.exit(1)
    
    def get_model_info(self, model_name):
        """Mendapatkan informasi model dari Odoo"""
        try:
            # Coba dengan field yang mungkin ada di versi Odoo yang berbeda
            fields = ['name']
            
            # Coba cek model untuk field yang mungkin berisi deskripsi
            model_info = self.models_api.execute_kw(
                self.db, self.uid, self.password,
                'ir.model', 'search_read',
                [[['model', '=', model_name]]],
                {'fields': fields}
            )
            
            if model_info:
                return model_info[0]
            return {'name': model_name}
        except Exception as e:
            print(f"Error getting model info: {str(e)}")
            return {'name': model_name}
    
    def get_model_fields(self, model_name):
        """Mendapatkan daftar field dan propertinya untuk model tertentu"""
        print(f"\nMengekstrak field untuk model: {model_name}")
        
        # Get fields from ir.model.fields
        field_ids = self.models_api.execute_kw(
            self.db, self.uid, self.password,
            'ir.model.fields', 'search',
            [[['model', '=', model_name]]]
        )
        
        fields_data = self.models_api.execute_kw(
            self.db, self.uid, self.password,
            'ir.model.fields', 'read',
            [field_ids],
            {'fields': ['name', 'field_description', 'ttype', 'required', 'relation', 'store', 'readonly', 'help']}
        )
        
        # Get model-specific field attributes
        model_fields = self.models_api.execute_kw(
            self.db, self.uid, self.password,
            model_name, 'fields_get',
            [],
            {'attributes': ['string', 'help', 'type', 'required', 'readonly', 'selection', 'relation', 'domain']}
        )
        
        # Enhance fields_data with additional model-specific attributes
        for field in fields_data:
            field_name = field['name']
            if field_name in model_fields:
                for attr, value in model_fields[field_name].items():
                    if attr not in field or not field[attr]:
                        field[attr] = value
        
        return fields_data
    
    def get_model_relationships(self, model_name, fields):
        """Ekstrak relasi antar model"""
        relationships = []
        
        for field in fields:
            if field['ttype'] in ['many2one', 'one2many', 'many2many'] and field.get('relation'):
                relationships.append({
                    'field_name': field['name'],
                    'field_description': field['field_description'],
                    'relation_type': field['ttype'],
                    'related_model': field['relation'],
                })
        
        return relationships
    
    def get_model_constraints(self, model_name):
        """Mendapatkan constraint untuk model"""
        try:
            constraints = self.models_api.execute_kw(
                self.db, self.uid, self.password,
                'ir.model.constraint', 'search_read',
                [[['model', '=', model_name]]],
                {'fields': ['name', 'message']}
            )
            return constraints
        except:
            return []
    
    def analyze_import_rules(self, model_name, fields):
        """Analisis aturan import untuk model"""
        required_fields = []
        recommended_fields = []
        computed_fields = []
        relation_fields = []
        
        for field in fields:
            # Skip technical fields
            if field['name'].startswith('_') or field['name'] in ['id', 'create_uid', 'create_date', 'write_uid', 'write_date', 'display_name']:
                continue
                
            is_stored = field.get('store', True)
            is_readonly = field.get('readonly', False)
            is_required = field.get('required', False)
            field_type = field.get('ttype')
            
            field_info = {
                'name': field['name'],
                'description': field['field_description'],
                'type': field_type,
                'help': field.get('help', '')
            }
            
            # Handling relational fields
            if field_type in ['many2one', 'one2many', 'many2many']:
                field_info['relation'] = field.get('relation')
                relation_fields.append(field_info)
                
                if field_type == 'many2one' and is_required:
                    required_fields.append(field_info)
            
            # Required and stored fields
            elif is_required and is_stored and not is_readonly:
                required_fields.append(field_info)
            
            # Computed fields
            elif is_readonly or not is_stored:
                computed_fields.append(field_info)
            
            # Recommended fields
            elif field['name'] in ['name', 'code', 'sequence', 'priority', 'state']:
                recommended_fields.append(field_info)
        
        return {
            'required_fields': required_fields,
            'recommended_fields': recommended_fields,
            'computed_fields': computed_fields,
            'relation_fields': relation_fields
        }
    
    def get_special_import_rules(self, model_name):
        """Dapatkan aturan import spesial untuk model tertentu"""
        rules = {}
        
        if model_name == 'mrp.bom':
            rules['bom_lines_import'] = (
                "Saat mengimpor BoM, komponen dapat didefinisikan dengan dua cara:\n"
                "1. Membuat record mrp.bom.line terpisah terlebih dahulu\n"
                "2. Menggunakan field 'bom_line_ids' atau kolom dengan format khusus di spreadsheet import\n"
                "   yang akan otomatis membuat record mrp.bom.line berdasarkan produk\n"
                "3. Format bom_line_ids: (0, 0, {'product_id': id, 'product_qty': qty})\n"
                "4. Saat melakukan import via CSV/XLS, product_id bisa berupa nama produk\n"
                "   dan Odoo akan mencari produk dengan nama tersebut"
            )
        
        elif model_name == 'sale.order':
            rules['order_lines_import'] = (
                "Saat mengimpor Sales Order, baris pesanan dapat didefinisikan dengan dua cara:\n"
                "1. Membuat record sale.order.line terpisah terlebih dahulu\n"
                "2. Menggunakan field 'order_line' atau kolom dengan format khusus di spreadsheet import\n"
                "   yang akan otomatis membuat record sale.order.line\n"
                "3. Format order_line: (0, 0, {'product_id': id, 'product_uom_qty': qty, 'price_unit': price})\n"
                "4. Saat melakukan import via CSV/XLS, product_id bisa berupa nama produk\n"
                "   dan Odoo akan mencari produk dengan nama tersebut"
            )
        
        elif model_name == 'product.product':
            rules['variants_import'] = (
                "product.product adalah varian produk dari product.template.\n"
                "1. Mengimpor ke product.product otomatis membuat product.template jika belum ada\n"
                "2. Untuk produk dengan varian, impor ke product.template terlebih dahulu,\n"
                "   kemudian impor varian ke product.product dengan mereferensikan template\n"
                "3. Field product_tmpl_id digunakan untuk mereferensikan template\n"
                "4. Atribut varian didefinisikan melalui field attribute_value_ids"
            )
        
        return rules
        
    def get_sample_data(self, model_name, limit=5, feb_2025=True):
        """Mendapatkan contoh data dari model tertentu"""
        print(f"\nMengambil contoh data untuk model: {model_name}")
        
        try:
            # Filter untuk data Februari 2025 jika diminta dan model memiliki field date
            domain = []
            if feb_2025:
                date_fields = []
                
                if model_name == 'sale.order':
                    date_fields = ['date_order']
                elif model_name == 'mrp.bom':
                    date_fields = ['create_date']
                elif model_name in ['product.product', 'product.template']:
                    date_fields = ['create_date']
                
                if date_fields:
                    for field in date_fields:
                        domain.append('|')
                    
                    domain = domain[:-1]  # Remove last OR
                    for field in date_fields:
                        domain.extend([(field, '>=', '2025-02-01'), (field, '<=', '2025-02-29')])
            
            # Get sample data
            sample_data = self.models_api.execute_kw(
                self.db, self.uid, self.password,
                model_name, 'search_read',
                [domain],
                {'limit': limit}
            )
            
            # If no February 2025 data found, try without date filter
            if not sample_data and feb_2025 and domain:
                print(f"  Tidak ada data Februari 2025 untuk {model_name}, mengambil data terbaru...")
                sample_data = self.models_api.execute_kw(
                    self.db, self.uid, self.password,
                    model_name, 'search_read',
                    [[]],
                    {'limit': limit}
                )
            
            return sample_data
            
        except Exception as e:
            print(f"Error mengambil contoh data untuk {model_name}: {str(e)}")
            return []
    
    def extract_model_schema(self, model_name):
        """Ekstrak skema lengkap untuk model tertentu"""
        print(f"\n{'='*50}")
        print(f"Memproses model: {model_name}")
        print(f"{'='*50}")
        
        try:
            # Get model information
            model_info = self.get_model_info(model_name)
            
            # Get fields
            fields = self.get_model_fields(model_name)
            
            # Get relationships
            relationships = self.get_model_relationships(model_name, fields)
            
            # Get constraints
            constraints = self.get_model_constraints(model_name)
            
            # Analyze import rules
            import_rules = self.analyze_import_rules(model_name, fields)
            
            # Get special import rules
            special_rules = self.get_special_import_rules(model_name)
            
            # Get sample data
            sample_data = self.get_sample_data(model_name, limit=5)
            
            # Organize result
            result = {
                'model': model_name,
                'name': model_info.get('name', model_name),
                'fields': fields,
                'relationships': relationships,
                'constraints': constraints,
                'import_rules': import_rules,
                'special_rules': special_rules,
                'sample_data': sample_data
            }
            
            # Add description if it exists
            if 'description' in model_info and model_info['description']:
                result['description'] = model_info['description']
            
            return result
        except Exception as e:
            print(f"Error extracting schema for {model_name}: {str(e)}")
            print("Continuing with next model...")
            return {
                'model': model_name,
                'name': model_name,
                'fields': [],
                'relationships': [],
                'constraints': [],
                'import_rules': {
                    'required_fields': [],
                    'recommended_fields': [],
                    'computed_fields': [],
                    'relation_fields': []
                },
                'special_rules': {},
                'sample_data': []
            }
    
    def generate_llm_friendly_text(self, schema_data):
        """Generate text output yang ramah untuk LLM"""
        output = []
        
        for model_name, model_data in schema_data.items():
            output.append(f"# Model: {model_data.get('name', model_name)} ({model_name})")
            if 'description' in model_data and model_data['description']:
                output.append(f"Deskripsi: {model_data['description']}")
            output.append("")
            
            # Fields
            output.append("## Fields:")
            for field in model_data['fields']:
                required = "* (REQUIRED)" if field.get('required') else ""
                readonly = " (readonly)" if field.get('readonly') else ""
                help_text = f"\n   Help: {field.get('help')}" if field.get('help') else ""
                relation = f" -> {field.get('relation')}" if field.get('relation') else ""
                
                output.append(f"- {field['name']}: {field['ttype']}{relation}{required}{readonly}")
                output.append(f"   Label: {field['field_description']}{help_text}")
            output.append("")
            
            # Relationships
            if model_data['relationships']:
                output.append("## Relationships:")
                for rel in model_data['relationships']:
                    output.append(f"- {rel['field_name']} ({rel['relation_type']}): {rel['related_model']}")
                    output.append(f"   Description: {rel['field_description']}")
                output.append("")
            
            # Constraints
            if model_data['constraints']:
                output.append("## Constraints:")
                for constraint in model_data['constraints']:
                    output.append(f"- {constraint['name']}: {constraint['message']}")
                output.append("")
            
            # Import Rules
            output.append("## Import Rules:")
            
            # Required fields
            if model_data['import_rules']['required_fields']:
                output.append("### Required Fields:")
                for field in model_data['import_rules']['required_fields']:
                    output.append(f"- {field['name']}: {field['description']} ({field['type']})")
            
            # Recommended fields
            if model_data['import_rules']['recommended_fields']:
                output.append("\n### Recommended Fields:")
                for field in model_data['import_rules']['recommended_fields']:
                    output.append(f"- {field['name']}: {field['description']} ({field['type']})")
            
            # Relational fields
            if model_data['import_rules']['relation_fields']:
                output.append("\n### Relational Fields:")
                for field in model_data['import_rules']['relation_fields']:
                    output.append(f"- {field['name']}: {field['description']} ({field['type']} -> {field.get('relation', '')})")
            
            # Special rules
            if model_data['special_rules']:
                output.append("\n### Special Import Rules:")
                for rule_name, rule_desc in model_data['special_rules'].items():
                    output.append(f"#### {rule_name}:")
                    output.append(rule_desc)
            
            # Sample Data
            if model_data.get('sample_data'):
                output.append("\n## Sample Data:")
                for i, record in enumerate(model_data['sample_data']):
                    output.append(f"\n### Record {i+1}:")
                    
                    # Tampilkan field-field penting saja untuk contoh
                    important_fields = [
                        'id', 'name', 'display_name', 'default_code', 'product_id', 
                        'product_tmpl_id', 'bom_id', 'order_id', 'product_qty',
                        'price_unit', 'date_order', 'partner_id', 'state'
                    ]
                    
                    # Tampilkan field penting terlebih dahulu
                    for field in important_fields:
                        if field in record:
                            value = record[field]
                            # Format nilai relasi
                            if isinstance(value, list) and len(value) == 2:
                                output.append(f"- {field}: {value[1]} (id: {value[0]})")
                            else:
                                output.append(f"- {field}: {value}")
                    
                    # Tampilkan field lain
                    for field, value in record.items():
                        if field not in important_fields:
                            # Format nilai relasi
                            if isinstance(value, list) and len(value) == 2:
                                output.append(f"- {field}: {value[1]} (id: {value[0]})")
                            else:
                                output.append(f"- {field}: {value}")
                    
                    # Batasi jumlah field yang ditampilkan agar tidak terlalu panjang
                    if len(record) > 10:
                        output.append("  (truncated for brevity)")
            
            output.append("\n" + "="*70 + "\n")
        
        return "\n".join(output)
    
    def export_to_text(self, data, filename):
        """Export data ke file text"""
        with open(filename, 'w') as f:
            f.write(data)
        print(f"Data diekspor ke {filename}")
    
    def export_to_json(self, data, filename):
        """Export data ke file JSON"""
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Data diekspor ke {filename}")
    
    def extract_all(self):
        """Ekstrak semua informasi skema"""
        all_data = {}
        
        for model in self.models:
            model_data = self.extract_model_schema(model)
            all_data[model] = model_data
        
        return all_data

def main():
    """Fungsi utama"""
    parser = argparse.ArgumentParser(description='Odoo Schema Extractor untuk LLM')
    
    parser.add_argument('--output', default='odoo_schema_for_llm.txt', help='Nama file output text')
    parser.add_argument('--json', default='odoo_schema.json', help='Nama file output JSON')
    
    args = parser.parse_args()
    
    print("Odoo Schema Extractor untuk LLM")
    print("-----------------------------")
    odoo_url = "https://api-odoo.visiniaga.com"
    database = "OdooDev"
    user = "odoo2@visiniaga.com"
    pwd = "PH8EQ?YF}<ac2A:T9n6%^*"
    # Initialize exporter
    exporter = OdooSchemaExporter(odoo_url, database, user, pwd)
    
    # Extract schema
    schema_data = exporter.extract_all()
    
    # Generate LLM-friendly text
    llm_text = exporter.generate_llm_friendly_text(schema_data)
    
    # Export to text and JSON
    exporter.export_to_text(llm_text, args.output)
    exporter.export_to_json(schema_data, args.json)
    
    print("\nEkstraksi selesai. Silakan periksa file output yang dihasilkan.")

if __name__ == "__main__":
    main()
