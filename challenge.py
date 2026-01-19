from main import NoSQLSimulator
from query_sim import QuerySimulator
import os

# Load DB1 (the best model) for the challenge
sim = NoSQLSimulator("schemas/db1.json", "stats.json")
qs = QuerySimulator(sim)

print("QUERY COST CALCULATION")
print("Using DB1 model \n")

#  Filter on category = 'smartphone' 
print("Query 1: SELECT description FROM Product WHERE categorie = 'smartphone'")

# Filter on Product collection
# Assume category is NOT the shard key → without sharding (worst case)
filter_q1 = qs.filter_without_sharding("Product")

# But if we had a shard key on category (low cardinality), it would be bad anyway
print("  Result (without sharding):")
print(f"    Docs scanned: {filter_q1['docs_scanned']:,}")
print(f"    Estimated time: {filter_q1['time_seconds']:,} seconds")
print(f"    Carbon footprint: {filter_q1['carbon_grams']:,} g CO₂")
print(f"    Estimated price: ${filter_q1['price_usd']:.4f}")

# Output size (rough: 5% of products have category 'smartphone')
output_docs_q1 = int(filter_q1['docs_scanned'] * 0.05)
output_size_gb_q1 = (output_docs_q1 * 200) / (1024**3)  # ~200 bytes per output row
print(f"    Output documents: ~{output_docs_q1:,}")
print(f"    Output size: ~{output_size_gb_q1:.4f} GB\n")

# QUERY 2: Join OrderLine + Product + filters 
print("Query 2: SELECT ol.quantity, p.price FROM OrderLine ol JOIN Product p ON ol.IDP = p.IDP")
print("         WHERE p.brand = 'Apple' AND ol.IDC = 125")

# Step 1: Filter Product on brand = 'Apple' (without sharding, since brand has low cardinality)
filter_product = qs.filter_without_sharding("Product")  # ~5,000 products for brand Apple (5%)

# Step 2: Join filtered Product with OrderLine on IDP (with sharding on IDP)
join_q2 = qs.join_with_sharding("OrderLine", "Product", "IDP")

# Step 3: Filter OrderLine on IDC = 125 (with sharding on IDC)
filter_orderline = qs.filter_with_sharding("OrderLine", "IDC")

# Total scanned = join scanned + filter scanned
total_scanned_q2 = join_q2['docs_scanned'] + filter_orderline['docs_scanned']
total_time = join_q2['time_seconds'] + filter_orderline['time_seconds']
total_carbon = join_q2['carbon_grams'] + filter_orderline['carbon_grams']
total_price = join_q2['price_usd'] + filter_orderline['price_usd']

print("  Result (with sharding on IDP & IDC):")
print(f"    Docs scanned (join + filter): {total_scanned_q2:,}")
print(f"    Estimated time: {total_time:,.0f} seconds")
print(f"    Carbon footprint: {total_carbon:,.0f} g CO₂")
print(f"    Estimated price: ${total_price:.4f}")

# Output size (rough: 1% of OrderLines match the filters)
output_docs_q2 = int(4100000000 * 0.01)  # ~41 million
output_size_gb_q2 = (output_docs_q2 * 150) / (1024**3)  # ~150 bytes per joined row
print(f"    Output documents: ~{output_docs_q2:,}")
print(f"    Output size: ~{output_size_gb_q2:.4f} GB")

print("\n" + "="*80)
print(" Costs estimated ")
print("="*80)