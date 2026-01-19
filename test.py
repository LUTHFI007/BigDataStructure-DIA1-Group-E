
from main import NoSQLSimulator             
from query_sim import QuerySimulator        
from aggregate_sim import AggregateSimulator  
import os                                 

print("=== BIG DATA STRUCTURE PROJECT  ===")


# Folder where the 5 db*.json files are stored
schemas_folder = "schemas"

# Find all db*.json files
db_files = [f for f in os.listdir(schemas_folder) 
            if f.startswith("db") and f.endswith(".json")]

if not db_files:
    print("ERROR: No db*.json files found in 'schemas' folder!")
    print("Expected: db1.json to db5.json")
    exit()

print(f"Found {len(db_files)} models: {', '.join(sorted(db_files))}\n")

# Store results for summary table
results = {}

# === SIZE & SHARDING ANALYSIS ===
for db_file in sorted(db_files):
    full_path = os.path.join(schemas_folder, db_file)
    db_name = db_file.replace('.json', '').upper()
    
    print(f"→ Analyzing {db_name} ({db_file})...")
    
    try:
        # Create simulator for this schema + shared stats
        sim = NoSQLSimulator(full_path, "stats.json")
        
        # Run full Chapter 2 analysis
        total_gb = sim.analyze_db(full_path)
        
        results[db_name] = total_gb
        
        print(f"  Success! Total size: {total_gb:.2f} GB\n")
    
    except Exception as e:
        print(f"  ERROR in {db_file}: {e}")
        print("  → Skipping. Check JSON format or path.\n")
        continue

# Chapter 2 Summary Table
print("\n" + "="*80)
print("              DB COMPARISON SUMMARY              ")
print("="*80)
print(f"{'DB':<8} {'Total Size (GB)':<18} {'Notes / Recommendation'}")
print("-"*90)

for db, size in sorted(results.items()):
    notes = ""
    if db == "DB1":
        notes = "Best model: Baseline (minimal denormalization, scalable)"
    elif db == "DB2":
        notes = "Prod + Stock array (slightly bigger, but deletes Stock)"
    elif db == "DB3":
        notes = "Stock nests Product (duplication, moderate increase)"
    elif db == "DB4":
        notes = "OrderLine nests Product (HUGE - 4.4 TB, dangerous)"
    elif db == "DB5":
        notes = "Product nests OrderLines (insane - 167 PB, impossible)"
    
    print(f"{db:<8} {size:<18.2f} {notes}")

print("\n" + "="*90)
print("FINAL RECOMMENDATION:")
print("→ DB1 is the most balanced, scalable, and realistic model.")
print("→ DB4 and DB5 show extreme denormalization causes storage explosion.")
print("="*90)

# === FILTER & JOIN SIMULATION (using DB1 as example) ===
print("\n" + "="*80)
print("FILTER & JOIN QUERIES + COSTS (using DB1)")
print("="*80)

qs = QuerySimulator(sim)

print("1. Filter Stock (with sharding on IDP):")
print(qs.filter_with_sharding("Stock", "IDP"))

print("\n2. Filter Stock (without sharding - full scan):")
print(qs.filter_without_sharding("Stock"))

print("\n3. Join OrderLine + Product (with sharding on IDP):")
print(qs.join_with_sharding("OrderLine", "Product", "IDP"))

print("\n4. Join OrderLine + Product (without sharding - very slow):")
print(qs.join_without_sharding("OrderLine", "Product"))

# === AGGREGATE QUERY SIMULATION (using DB1) ===
print("\n" + "="*80)
print("AGGREGATE QUERIES + COSTS (using DB1)")
print("="*80)

aggs = AggregateSimulator(sim)

print("Aggregate on OrderLine (with sharding on IDP):")
print(aggs.simulate_aggregate("OrderLine", "IDP"))

print("\nAggregate on OrderLine (without sharding - full scan):")
print(aggs.simulate_aggregate("OrderLine"))

print("\n" + "="*80)
print("="*80)

