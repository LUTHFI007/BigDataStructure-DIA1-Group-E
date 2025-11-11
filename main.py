import json
import os
from typing import Dict, List

class NoSQLSimulator:
    def __init__(self, stats_file: str):
        with open(stats_file, 'r', encoding='utf-8') as f:
            self.stats = json.load(f)
        
        self.sizes = {
            "integer": 8,
            "number": 8,
            "string": 80,
            "date": 20,
            "longstring": 200
        }
        self.servers = 1000

    def load_schema(self, schema_file: str):
        """Load a single DB schema file"""
        with open(schema_file, 'r', encoding='utf-8') as f:
            self.schemas = json.load(f)
        return self.schemas

    def get_collection_schema(self, name: str):
        for coll in self.schemas:
            if coll["collection"] == name:
                return coll["properties"]
        return None

    def compute_field_size(self, field_type: str, field_name: str = "") -> int:
        if field_type in self.sizes:
            return self.sizes[field_type]
        return 80  # default

    def compute_doc_size(self, collection: str) -> int:
        schema = self.get_collection_schema(collection)
        if not schema:
            return 0
        
        size = 0
        
        for field, spec in schema.items():
            ftype = spec["type"]
            
            # Key overhead: 12B per field
            size += 12

            if ftype in ["integer", "number", "string", "date", "longstring"]:
                size += self.compute_field_size(ftype, field)
            
            elif ftype == "object":
                obj_props = spec["properties"]
                for subfield, subspec in obj_props.items():
                    size += 12 + self.compute_field_size(subspec["type"], subfield)
            
            elif ftype == "array":
                avg_count = self.stats["avg"].get(collection, {}).get(field, 1)
                item_props = spec["items"]["properties"]
                item_size = 0
                for subfield, subspec in item_props.items():
                    item_size += 12 + self.compute_field_size(subspec["type"])
                size += 12 + avg_count * item_size  # array overhead + items

        return size

    def collection_size_gb(self, collection: str) -> float:
        doc_size = self.compute_doc_size(collection)
        n_docs = self.stats["cardinality"].get(collection, 0)
        return (n_docs * doc_size) / (1024**3)

    def database_size_gb(self) -> float:
        total = 0
        for coll in self.stats["cardinality"]:
            total += self.collection_size_gb(coll)
        return total

    def sharding_stats(self, collection: str, shard_key: str):
        n_docs = self.stats["cardinality"].get(collection, 0)
        n_keys = self.stats["distinct"].get(collection, {}).get(shard_key, n_docs)
        return {
            "docs_per_server": n_docs // self.servers,
            "distinct_keys_per_server": n_keys // self.servers
        }

    def analyze_db(self, db_file: str):
        """Analyze one DB file"""
        self.load_schema(db_file)
        db_name = os.path.basename(db_file).replace('.json', '').upper()
        
        print(f"\n=== {db_name} ANALYSIS ===")
        
        # Document sizes
        print("DOCUMENT SIZES:")
        for coll in ["Product", "Stock", "OrderLine", "Client", "Warehouse"]:
            if self.get_collection_schema(coll):
                size = self.compute_doc_size(coll)
                print(f"  {coll}: {size} bytes")
        
        # Collection sizes
        print("\nCOLLECTION SIZES (GB):")
        total_gb = 0
        for coll in self.stats["cardinality"]:
            if self.get_collection_schema(coll):
                size_gb = self.collection_size_gb(coll)
                print(f"  {coll}: {size_gb:.3f} GB")
                total_gb += size_gb
        print(f"  TOTAL DB: {total_gb:.2f} GB")
        
        # Sharding 
        print("\nSHARDING EXAMPLES:")
        if self.get_collection_schema("Stock"):
            print(f"  Stock - IDP: {self.sharding_stats('Stock', 'IDP')}")
        if self.get_collection_schema("OrderLine"):
            print(f"  OrderLine - IDC: {self.sharding_stats('OrderLine', 'IDC')}")
        if self.get_collection_schema("Product"):
            print(f"  Product - brand: {self.sharding_stats('Product', 'brand')}")
        
        return total_gb