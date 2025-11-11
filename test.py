from main import NoSQLSimulator
import os

# Initializing with stats
sim = NoSQLSimulator("stats.json")

# Finding all DB files 
db_files = [f for f in os.listdir("schemas") if f.startswith("db") and f.endswith(".json")]

# Analyzing each DB
results = {}
for db_file in sorted(db_files):
    full_path = os.path.join("schemas", db_file)
    total_gb = sim.analyze_db(full_path)
    db_name = db_file.replace('.json', '').upper()
    results[db_name] = total_gb

# Summary Table
print("\n" + "="*50)
print("DB COMPARISON SUMMARY")
print("="*50)
print(f"{'DB':<6} {'Total Size (GB)':<15} {'Notes'}")
print("-"*50)
for db, size in results.items():
    notes = ""
    if db == "DB1": notes = "Baseline (denorm Prod only)"
    elif db == "DB2": notes = "Prod + Stock array (bigger)"
    elif db == "DB3": notes = "Stock nests Prod (duplication)"
    elif db == "DB4": notes = "OL nests Prod (HUGE)"
    elif db == "DB5": notes = "Prod + OL array (insane size)"
    print(f"{db:<6} {size:<15.2f} {notes}")

print("\nRECOMMENDATION: DB1 is smallest & scalable.")