import json
from nlp_sql import run_nl_query, generate_dynamic_schema

def run_test(db_path: str, queries: list):
    """
    Test the run_nl_query function from nlp_sql.py with a list of natural language queries.
    Prints the database schema and the results for each query.
    
    Args:
        db_path (str): Path to the SQLite database file.
        queries (list): List of natural language queries to test.
    """
    print(f"\nTesting database: {db_path}")
    try:
        schema = generate_dynamic_schema(db_path)
        print("Schema:\n", schema)
    except Exception as e:
        print(f"Error generating schema: {e}")
        return

    for query in queries:
        print(f"\n=== Query: {query} ===")
        try:
            result = run_nl_query(query, db_path)
            print("Result:")
            print(json.dumps(result, indent=2))
        except Exception as e:
            print(f"Error processing query '{query}': {e}")

if __name__ == "__main__":
    db_path = "company.db"
    queries = [
        # Simple FTS query
        # "Show products like Model S",
        # # Complex queries from provided script
        #"Show the top 5 regions by total revenue for EV products in 2025, including the number of unique customers and average sale amount per transaction, only for sales where the product stock is below 100",
         "Show products with sales in 2025 where the total revenue exceeds the average revenue of all products in their category, including the total quantity sold and the percentage of total revenue for that category",
        # # Edge case: Invalid query
        # "Drop the products table",  # Should be caught by security checks
        # # Edge case: Likely empty result
        # "Show products like NonExistentProduct",
        # # Additional FTS query
        # "Find products with Tesla in name"
    ]
    
    run_test(db_path, queries)


# import pandas as pd
# from datetime import datetime

# def update_csvs():
#     current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
#     try:
#         # Load existing products
#         products_df = pd.read_csv("data/products_full_tesla.csv")
#         existing_product_ids = set(products_df['product_id'])
#         print(f"Existing product_id range: {min(existing_product_ids)}–{max(existing_product_ids)}")

#         # Create placeholder entries for product_id 107–139
#         new_products = pd.DataFrame([
#             {
#                 'product_id': pid,
#                 'name': f'Tesla Product {pid}',
#                 'category': 'Unknown',
#                 'price': 0.0,
#                 'stock': 0,
#                 'supplier': 'Unknown',
#                 'tags': 'placeholder',
#                 'created_at': current_time,
#                 'updated_at': current_time
#             }
#             for pid in range(107, 140) if pid not in existing_product_ids
#         ])
        
#         # Combine existing and new products
#         updated_products = pd.concat([products_df, new_products]).drop_duplicates(subset=['product_id'])
#         updated_products = updated_products[['product_id', 'name', 'category', 'price', 'stock', 'supplier', 'tags', 'created_at', 'updated_at']]
#         updated_products.to_csv("data/products_full_tesla_updated.csv", index=False)
#         print(f"Created products_full_tesla_updated.csv with product_id range: {min(updated_products['product_id'])}–{max(updated_products['product_id'])}")

#         # Load materials and deduplicate
#         materials_df = pd.read_csv("data/materials_tesla.csv")
#         dedup_materials = materials_df.drop(columns=['tags', 'product_id', 'material_id'], errors='ignore')
#         dedup_materials = dedup_materials.drop_duplicates(subset=['material_name', 'material_type'])
#         dedup_materials['material_id'] = range(1, len(dedup_materials) + 1)
#         dedup_materials['created_at'] = dedup_materials.get('created_at', current_time)
#         dedup_materials['updated_at'] = dedup_materials.get('updated_at', current_time)

#         # Map original material_id to new material_id
#         material_id_map = {(row['material_name'], row['material_type']): row['material_id'] 
#                           for _, row in dedup_materials.iterrows()}
        
#         # Update materials_tesla.csv with new material_id and validate product_id
#         updated_materials = materials_df.copy()
#         updated_materials['new_material_id'] = updated_materials.apply(
#             lambda x: material_id_map.get((x['material_name'], x['material_type'])), axis=1
#         )
#         valid_product_ids = set(updated_products['product_id'])
#         invalid_rows = updated_materials[~updated_materials['product_id'].isin(valid_product_ids)]
#         if not invalid_rows.empty:
#             invalid_rows.to_csv("data/invalid_materials_rows.csv", index=False)
#             print(f"Invalid product_id rows saved to 'data/invalid_materials_rows.csv'")
        
#         updated_materials = updated_materials[updated_materials['product_id'].isin(valid_product_ids)]
#         updated_materials = updated_materials.rename(columns={'new_material_id': 'material_id'})
#         # Select columns, adding created_at/updated_at if missing
#         output_columns = ['material_id', 'material_name', 'material_type', 'mass', 'product_id', 'tags']
#         if 'created_at' in materials_df.columns:
#             output_columns.append('created_at')
#         else:
#             updated_materials['created_at'] = current_time
#             output_columns.append('created_at')
#         if 'updated_at' in materials_df.columns:
#             output_columns.append('updated_at')
#         else:
#             updated_materials['updated_at'] = current_time
#             output_columns.append('updated_at')
#         updated_materials = updated_materials[output_columns]
#         updated_materials.to_csv("data/materials_tesla_updated.csv", index=False)
#         print("Created materials_tesla_updated.csv with deduplicated material_id and validated product_id")

#         # Inspect bom_tesla.csv (for reference)
#         try:
#             bom_df = pd.read_csv("data/bom_tesla.csv")
#             print(f"Columns in bom_tesla.csv: {list(bom_df.columns)}")
#             print(f"Sample bom_tesla.csv:\n{bom_df.head().to_string(index=False)}")
#         except FileNotFoundError:
#             print("Warning: 'bom_tesla.csv' not found. Ensure correct columns when available.")
#         except Exception as e:
#             print(f"Error reading bom_tesla.csv: {e}")

#     except FileNotFoundError as e:
#         print(f"Error: {e}")
#     except Exception as e:
#         print(f"Unexpected error: {e}")

# if __name__ == "__main__":
#     update_csvs()

# import pandas as pd

# def inspect_bom_csv():
#     try:
#         bom_df = pd.read_csv("data/bom_tesla.csv")
#         print(f"Columns in bom_tesla.csv: {list(bom_df.columns)}")
#         print(f"Sample data (first 5 rows):\n{bom_df.head().to_string(index=False)}")
#         print(f"Total rows: {len(bom_df)}")
#     except FileNotFoundError:
#         print("Error: 'bom_tesla.csv' not found in data/ directory.")
#     except Exception as e:
#         print(f"Error reading bom_tesla.csv: {e}")

# if __name__ == "__main__":
#     inspect_bom_csv()