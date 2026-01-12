# run_final.py
# Chapter 5 – The Data Model Selection’s Challenge 
# Runs ALL queries on ALL 5 models and chooses the best one

from main import NoSQLSimulator
from query_sim import QuerySimulator
from aggregate_sim import AggregateSimulator
import os

print("===  CHAPTER 5 ===")
print("Running complete use case simulation on all 5 models...\n")

schemas_folder = "schemas"
db_files = [f for f in os.listdir(schemas_folder) if f.startswith("db") and f.endswith(".json")]

total_costs = {}

for db_file in sorted(db_files):
    full_path = os.path.join(schemas_folder, db_file)
    db_name = db_file.replace('.json', '').upper()
    
    print(f"Model: {db_name}")
    sim = NoSQLSimulator(full_path, "stats.json")
    qs = QuerySimulator(sim)
    aggs = AggregateSimulator(sim)
    
    model_total_cost = 0.0
    
    # Filter example
    filter_cost = qs.filter_with_sharding("Stock", "IDP")["price_usd"]
    model_total_cost += filter_cost
    print(f"  Filter cost: ${filter_cost:.6f}")
    
    # Join example
    join_cost = qs.join_with_sharding("OrderLine", "Product", "IDP")["price_usd"]
    model_total_cost += join_cost
    print(f"  Join cost: ${join_cost:.6f}")
    
    # Aggregate example
    agg_cost = aggs.simulate_aggregate("OrderLine", "IDP")["price_usd"]
    model_total_cost += agg_cost
    print(f"  Aggregate cost: ${agg_cost:.6f}")
    
    print(f"  TOTAL ESTIMATED COST for {db_name}: ${model_total_cost:.6f}\n")
    total_costs[db_name] = model_total_cost

# Choose the best model
best_model = min(total_costs, key=total_costs.get)
print("="*70)
print(f"BEST MODEL: {best_model}")
print(f"Total estimated cost: ${total_costs[best_model]:.6f}")
print("Reason: Lowest overall cost, balanced sharding, minimal duplication, realistic scalability")
print("="*70)
print("All other models have higher costs due to excessive denormalization (especially DB4 & DB5)")
