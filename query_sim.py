# query_sim.py
# Filter and Join Queries + Cost Estimation
# This file adds the 4 required operators 

class QuerySimulator:
    def __init__(self, nosql_simulator):
        """
        Connects to your main NoSQLSimulator (from main.py)
        to reuse stats, servers, etc.
        """
        self.sim = nosql_simulator

    def filter_with_sharding(self, collection, shard_key):
        """
        Filter query using sharding key → only 1 server is scanned (fast)
        Example: Find all stock for a specific product ID
        """
        total_docs = self.sim.stats.get("cardinality", {}).get(collection, 0)
        
        # With sharding: only 1 server
        docs_scanned = total_docs // self.sim.servers
        
        # Cost estimation (simple approximations)
        time_seconds = docs_scanned * 0.01      # 10 ms per document scanned
        carbon_grams = docs_scanned * 0.0001    # rough grams CO₂
        price_usd = docs_scanned * 0.000001     # rough $ per doc
        
        return {
            "query_type": "filter_with_sharding",
            "collection": collection,
            "shard_key": shard_key,
            "docs_scanned": docs_scanned,
            "time_seconds": round(time_seconds, 2),
            "carbon_grams": round(carbon_grams, 2),
            "price_usd": round(price_usd, 4)
        }

    def filter_without_sharding(self, collection):
        """
        Filter query without sharding → full scan of ALL documents (slow)
        """
        total_docs = self.sim.stats.get("cardinality", {}).get(collection, 0)
        
        # Without sharding: scan everything
        docs_scanned = total_docs
        
        time_seconds = docs_scanned * 0.01
        carbon_grams = docs_scanned * 0.0001
        price_usd = docs_scanned * 0.000001
        
        return {
            "query_type": "filter_without_sharding",
            "collection": collection,
            "docs_scanned": docs_scanned,
            "time_seconds": round(time_seconds, 2),
            "carbon_grams": round(carbon_grams, 2),
            "price_usd": round(price_usd, 4)
        }

    def join_with_sharding(self, coll1, coll2, shard_key):
        """
        Nested loop join with sharding → only scans 1 server (fast)
        Example: Join OrderLine with Product on IDP
        """
        docs1 = self.sim.stats.get("cardinality", {}).get(coll1, 0)
        docs2 = self.sim.stats.get("cardinality", {}).get(coll2, 0)
        
        # With sharding: scan only the larger collection on 1 server
        docs_scanned = max(docs1, docs2) // self.sim.servers
        
        time_seconds = docs_scanned * 0.05      # Joins are slower (~50 ms/doc)
        carbon_grams = docs_scanned * 0.0005
        price_usd = docs_scanned * 0.000005
        
        return {
            "query_type": "join_with_sharding",
            "collections": f"{coll1} + {coll2}",
            "shard_key": shard_key,
            "docs_scanned": docs_scanned,
            "time_seconds": round(time_seconds, 2),
            "carbon_grams": round(carbon_grams, 2),
            "price_usd": round(price_usd, 6)
        }

    def join_without_sharding(self, coll1, coll2):
        """
        Nested loop join without sharding → full cross-product (very slow & expensive)
        """
        docs1 = self.sim.stats.get("cardinality", {}).get(coll1, 0)
        docs2 = self.sim.stats.get("cardinality", {}).get(coll2, 0)
        
        # Without sharding: full nested loop (docs1 × docs2)
        docs_scanned = docs1 * docs2
        
        time_seconds = docs_scanned * 0.05
        carbon_grams = docs_scanned * 0.0005
        price_usd = docs_scanned * 0.000005
        
        return {
            "query_type": "join_without_sharding",
            "collections": f"{coll1} + {coll2}",
            "docs_scanned": docs_scanned,
            "time_seconds": round(time_seconds, 2),
            "carbon_grams": round(carbon_grams, 2),
            "price_usd": round(price_usd, 6)
        }
    
