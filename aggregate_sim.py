# aggregate_sim.py
# Aggregate Queries + Costs 

class AggregateSimulator:
    def __init__(self, nosql_simulator):
        """Connects to the main NoSQLSimulator"""
        self.sim = nosql_simulator

    def simulate_aggregate(self, collection, shard_key=None):
        """Simulate aggregate query (group by, sum, count, etc.)"""
        total_docs = self.sim.stats.get("cardinality", {}).get(collection, 0)
        
        if shard_key:
            docs_scanned = total_docs // self.sim.servers  # Sharded: 1 server
        else:
            docs_scanned = total_docs  # Full scan
        
        # Output size (aggregates return fewer rows, e.g., groups or top N)
        output_docs = max(1, docs_scanned // 100)  # e.g. top 100 or groups
        output_size_gb = (output_docs * 200) / (1024**3)  # rough 200 bytes per row
        
        time_seconds = docs_scanned * 0.03      # Aggregates medium speed
        carbon_grams = docs_scanned * 0.0003
        price_usd = docs_scanned * 0.000003
        
        return {
            "query_type": "aggregate",
            "collection": collection,
            "sharded": bool(shard_key),
            "docs_scanned": docs_scanned,
            "output_docs": output_docs,
            "output_size_gb": round(output_size_gb, 4),
            "time_seconds": round(time_seconds, 2),
            "carbon_grams": round(carbon_grams, 2),
            "price_usd": round(price_usd, 6)
        }