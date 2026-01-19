import json
import os
from typing import Dict, List, Any

class NoSQLSimulator:
    def __init__(self, schema_file: str, stats_file: str):
        """
        Initialize the simulator with a schema file (e.g., db1.json) and stats file.
        """
        self.load_schema(schema_file)
        with open(stats_file, 'r', encoding='utf-8') as f:
            self.stats = json.load(f)
        
        self.servers = 1000
        self.field_sizes = {
            "integer": 8,
            "number": 8,
            "string": 80,
            "date": 20,
            "longstring": 200
        }

    def load_schema(self, schema_file: str):
        """Load the JSON schema file (db1.json, db2.json, etc.)"""
        with open(schema_file, 'r', encoding='utf-8') as f:
            self.schemas = json.load(f)

    def get_collection_schema(self, name: str):
        """Find the schema for a specific collection (Product, Stock, etc.)"""
        for coll in self.schemas:
            if coll.get("collection") == name:
                return coll.get("properties", {})
        return {}

    def compute_field_size(self, field_type: str, field_name: str = "") -> int:
        """Return size for basic field types"""
        if field_type in self.field_sizes:
            return self.field_sizes[field_type]
        return 80  # default fallback

    def compute_doc_size(self, collection: str) -> int:
        """Calculate size of one document in bytes """
        schema = self.get_collection_schema(collection)
        if not schema:
            return 0
        
        size = 0
        
        for field, spec in schema.items():
            ftype = spec.get("type")
            if not ftype:
                continue
                
            # Every field has 12B key overhead 
            size += 12

            if ftype in ["integer", "number", "string", "date", "longstring"]:
                size += self.compute_field_size(ftype, field)
            
            elif ftype == "object":
                # Nested object (e.g. price, supplier)
                obj_props = spec.get("properties", {})
                for subfield, subspec in obj_props.items():
                    size += 12 + self.compute_field_size(subspec.get("type", "string"), subfield)
            
            elif ftype == "array":
                # Array (e.g. categories, orderlines)
                avg_count = self.stats.get("avg", {}).get(collection, {}).get(field, 1)
                item_props = spec.get("items", {}).get("properties", {})
                item_size = 0
                for subfield, subspec in item_props.items():
                    item_size += 12 + self.compute_field_size(subspec.get("type", "string"), subfield)
                size += 12 + avg_count * item_size  # array overhead + items

        return size

    def collection_size_gb(self, collection: str) -> float:
        """Size of one collection in GB"""
        doc_size = self.compute_doc_size(collection)
        n_docs = self.stats.get("cardinality", {}).get(collection, 0)
        return round((n_docs * doc_size) / (1024 ** 3), 3)

    def database_size_gb(self) -> float:
        """Total database size in GB across all collections"""
        total = 0
        for coll in self.schemas:
            coll_name = coll.get("collection")
            if coll_name:
                total += self.collection_size_gb(coll_name)
        return round(total, 2)

    def sharding_stats(self, collection: str, shard_key: str) -> Dict[str, int]:
        """Simulate sharding over 1000 servers"""
        n_docs = self.stats.get("cardinality", {}).get(collection, 0)
        n_keys = self.stats.get("distinct", {}).get(collection, {}).get(shard_key, n_docs)
        return {
            "docs_per_server": n_docs // self.servers,
            "distinct_keys_per_server": n_keys // self.servers
        }

    def analyze_db(self, db_file: str) -> float:
        """
        Analyze one database schema file.
        This method is called by test.py loop.
        """
        self.load_schema(db_file)
        db_name = os.path.basename(db_file).replace('.json', '').upper()
        
        print(f"\n=== {db_name} ANALYSIS ===")
        
        # Document sizes
        print("DOCUMENT SIZES:")
        collections = ["Product", "Stock", "OrderLine", "Client", "Warehouse"]
        for coll in collections:
            if self.get_collection_schema(coll):
                size = self.compute_doc_size(coll)
                print(f"  {coll}: {size} bytes")
        
        # Collection sizes
        print("\nCOLLECTION SIZES (GB):")
        total_gb = 0
        for coll in self.stats.get("cardinality", {}):
            if self.get_collection_schema(coll):
                size_gb = self.collection_size_gb(coll)
                print(f"  {coll}: {size_gb:.3f} GB")
                total_gb += size_gb
        print(f"  TOTAL DB: {total_gb:.2f} GB")
        
        # Sharding examples
        print("\nSHARDING EXAMPLES:")
        if self.get_collection_schema("Stock"):
            print(f"  Stock - IDP: {self.sharding_stats('Stock', 'IDP')}")
        if self.get_collection_schema("OrderLine"):
            print(f"  OrderLine - IDC: {self.sharding_stats('OrderLine', 'IDC')}")
        if self.get_collection_schema("Product"):
            print(f"  Product - brand: {self.sharding_stats('Product', 'brand')}")
        
        return total_gb
    
        # ──────────────── FILTER & JOIN SIMULATION ────────────────

    def filter_with_sharding(self, collection, shard_key):
        """Fast filter – uses sharding key → only 1 server scanned"""
        total_docs = self.stats.get("cardinality", {}).get(collection, 0)
        docs_scanned = total_docs // self.servers
        
        time_s = docs_scanned * 0.01      # 10 ms per document scanned
        carbon_g = docs_scanned * 0.0001  # rough gCO2 per doc
        price_usd = docs_scanned * 0.000001
        
        return {
            "query_type": "filter_with_sharding",
            "docs_scanned": docs_scanned,
            "time_seconds": round(time_s, 2),
            "carbon_grams": round(carbon_g, 2),
            "price_usd": round(price_usd, 4)
        }

    def filter_without_sharding(self, collection):
        """Slow filter – full scan of all documents"""
        total_docs = self.stats.get("cardinality", {}).get(collection, 0)
        docs_scanned = total_docs
        
        time_s = docs_scanned * 0.01
        carbon_g = docs_scanned * 0.0001
        price_usd = docs_scanned * 0.000001
        
        return {
            "query_type": "filter_without_sharding",
            "docs_scanned": docs_scanned,
            "time_seconds": round(time_s, 2),
            "carbon_grams": round(carbon_g, 2),
            "price_usd": round(price_usd, 4)
        }

    def join_with_sharding(self, coll1, coll2, shard_key):
        """Fast join – only scans 1 server (assumes join key exists)"""
        docs1 = self.stats.get("cardinality", {}).get(coll1, 0)
        docs2 = self.stats.get("cardinality", {}).get(coll2, 0)
        docs_scanned = max(docs1, docs2) // self.servers
        
        time_s = docs_scanned * 0.05      # Joins are slower (~50 ms/doc)
        carbon_g = docs_scanned * 0.0005
        price_usd = docs_scanned * 0.000005
        
        return {
            "query_type": "join_with_sharding",
            "docs_scanned": docs_scanned,
            "time_seconds": round(time_s, 2),
            "carbon_grams": round(carbon_g, 2),
            "price_usd": round(price_usd, 6)
        }

    def join_without_sharding(self, coll1, coll2):
        """Slow join – full nested loop (very expensive)"""
        docs1 = self.stats.get("cardinality", {}).get(coll1, 0)
        docs2 = self.stats.get("cardinality", {}).get(coll2, 0)
        docs_scanned = docs1 * docs2
        
        time_s = docs_scanned * 0.05
        carbon_g = docs_scanned * 0.0005
        price_usd = docs_scanned * 0.000005
        
        return {
            "query_type": "join_without_sharding",
            "docs_scanned": docs_scanned,
            "time_seconds": round(time_s, 2),
            "carbon_grams": round(carbon_g, 2),
            "price_usd": round(price_usd, 6)
        }