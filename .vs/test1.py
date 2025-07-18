# import csv

# # Define material mappings based on product categories and material types
# material_mappings = {
#     "EV Car": [
#         {"material_id": 5, "material_name": "Cobalt", "material_type": "Battery Mineral"},
#         {"material_id": 6, "material_name": "Nickel", "material_type": "Battery Mineral"},
#         {"material_id": 9, "material_name": "Lithium", "material_type": "Battery Mineral"},
#         {"material_id": 8, "material_name": "Copper", "material_type": "Electrical"},
#         {"material_id": 4, "material_name": "Silicon", "material_type": "Semiconductor"},
#         {"material_id": 2, "material_name": "Glass", "material_type": "Structural"},
#         {"material_id": 3, "material_name": "Steel", "material_type": "Chassis"},
#         {"material_id": 10, "material_name": "Aluminium", "material_type": "Metal"},
#         {"material_id": 7, "material_name": "Plastic", "material_type": "Housing"}
#     ],
#     "EV Truck": [
#         {"material_id": 5, "material_name": "Cobalt", "material_type": "Battery Mineral"},
#         {"material_id": 6, "material_name": "Nickel", "material_type": "Battery Mineral"},
#         {"material_id": 9, "material_name": "Lithium", "material_type": "Battery Mineral"},
#         {"material_id": 8, "material_name": "Copper", "material_type": "Electrical"},
#         {"material_id": 4, "material_name": "Silicon", "material_type": "Semiconductor"},
#         {"material_id": 3, "material_name": "Steel", "material_type": "Chassis"},
#         {"material_id": 10, "material_name": "Aluminium", "material_type": "Metal"},
#         {"material_id": 7, "material_name": "Plastic", "material_type": "Housing"}
#     ],
#     "Sports Car": [
#         {"material_id": 5, "material_name": "Cobalt", "material_type": "Battery Mineral"},
#         {"material_id": 6, "material_name": "Nickel", "material_type": "Battery Mineral"},
#         {"material_id": 9, "material_name": "Lithium", "material_type": "Battery Mineral"},
#         {"material_id": 8, "material_name": "Copper", "material_type": "Electrical"},
#         {"material_id": 4, "material_name": "Silicon", "material_type": "Semiconductor"},
#         {"material_id": 10, "material_name": "Aluminium", "material_type": "Metal"},
#         {"material_id": 7, "material_name": "Plastic", "material_type": "Housing"},
#         {"material_id": 1, "material_name": "Rare Earth Elements", "material_type": "Magnets"}
#     ],
#     "Unknown": [
#         {"material_id": 7, "material_name": "Plastic", "material_type": "Housing"},
#         {"material_id": 8, "material_name": "Copper", "material_type": "Electrical"}
#     ]
# }

# # Load materials data to get mass values
# materials = {}
# with open("data/materials_tesla.csv", "r") as f:
#     reader = csv.DictReader(f)
#     for row in reader:
#         materials[int(row["material_id"])] = float(row["mass"])

# # Load products data to get product IDs and categories
# products = []
# with open("data/products_full_tesla.csv", "r") as f:
#     reader = csv.DictReader(f)
#     for row in reader:
#         products.append({"product_id": int(row["product_id"]), "category": row["category"]})

# # Generate bill of materials data
# bom_data = []
# bom_id = 1
# for product in products:
#     product_id = product["product_id"]
#     category = product["category"]
#     for material in material_mappings.get(category, []):
#         material_id = material["material_id"]
#         # Use the mass from materials dataset as quantity
#         quantity = materials.get(material_id, 1.0)  # Default to 1.0 if not found
#         bom_data.append({
#             "bom_id": bom_id,
#             "product_id": product_id,
#             "material_id": material_id,
#             "subassembly_id": None,  # No subassemblies in this dataset
#             "quantity": quantity,
#             "unit": "kg"
#         })
#         bom_id += 1

# # Write to CSV
# with open("data/bom_tesla.csv", "w", newline="") as f:
#     writer = csv.DictWriter(f, fieldnames=["bom_id", "product_id", "material_id", "subassembly_id", "quantity", "unit"])
#     writer.writeheader()
#     for entry in bom_data:
#         writer.writerow(entry)

# print("data/bom_tesla.csv has been generated successfully.")


# import pandas as pd
# import sqlite3
# import logging


# # Set up logging
# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# def separate_tags(csv_file_path, db_path):
#     try:
#         # Read the CSV file
#         logging.info(f"Reading CSV file: {csv_file_path}")
#         df = pd.read_csv(csv_file_path)
        
#         # Check if required columns exist
#         if 'product_id' not in df.columns or 'tags' not in df.columns:
#             logging.error("CSV must contain 'product_id' and 'tags' columns")
#             return
        
#         # Connect to SQLite database
#         logging.info(f"Connecting to database: {db_path}")
#         conn = sqlite3.connect(db_path)
#         cursor = conn.cursor()
        
#         # Process each row
#         for index, row in df.iterrows():
#             product_id = row['product_id']
#             tags = row['tags']
            
#             # Skip if tags is NaN or empty
#             if pd.isna(tags) or not tags:
#                 logging.warning(f"No tags for product_id {product_id}, skipping")
#                 continue
            
#             # Split comma-separated tags and clean them
#             tag_list = [tag.strip() for tag in tags.split(',')]
#             # Standardize 'ev' to 'EV' for query compatibility
#             tag_list = ['EV' if tag.lower() == 'ev' else tag for tag in tag_list]
            
#             # Delete existing tags for this product_id
#             cursor.execute("DELETE FROM product_tags WHERE product_id = ?", (product_id,))
#             logging.info(f"Deleted existing tags for product_id {product_id}")
            
#             # Insert new tags
#             for tag in tag_list:
#                 try:
#                     cursor.execute(
#                         "INSERT INTO product_tags (product_id, tag) VALUES (?, ?)",
#                         (product_id, tag)
#                     )
#                     logging.info(f"Inserted tag '{tag}' for product_id {product_id}")
#                 except sqlite3.IntegrityError as e:
#                     logging.warning(f"Failed to insert tag '{tag}' for product_id {product_id}: {e}")
            
#         # Commit changes and close connection
#         conn.commit()
#         logging.info("Changes committed to database")
        
#         # Verify inserted tags for product_id 122, 133
#         cursor.execute("SELECT product_id, tag FROM product_tags WHERE product_id IN (122, 133)")
#         results = cursor.fetchall()
#         logging.info("Verification results for product_id 122, 133:")
#         for result in results:
#             logging.info(f"product_id: {result[0]}, tag: {result[1]}")
        
#     except Exception as e:
#         logging.error(f"An error occurred: {e}")
#     finally:
#         conn.close()
#         logging.info("Database connection closed")

# if __name__ == "__main__":
#     # Replace with actual paths
#     csv_file_path = "data/products_full_tesla.csv"
#     db_path = "data/company.db"
#     separate_tags(csv_file_path, db_path)



# import pandas as pd
# import os
# from datetime import datetime

# def clean_and_separate_data():
#     """
#     Clean the CSV files and separate tags into dedicated files according to the schema
#     """
    
#     # Create output directory if it doesn't exist
#     output_dir = "data_cleaned"
#     if not os.path.exists(output_dir):
#         os.makedirs(output_dir)
    
#     current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
#     print("Starting data cleanup and tag separation...")
    
#     # ===== CLEAN PRODUCTS DATA =====
#     print("\n1. Processing products data...")
#     try:
#         products_df = pd.read_csv("data/products_full_tesla.csv")
#         print(f"Original products data shape: {products_df.shape}")
        
#         # Extract tags before cleaning
#         product_tags_data = []
#         for _, row in products_df.iterrows():
#             product_id = row['product_id']
#             tags = row['tags'].split(',') if pd.notna(row['tags']) else []
#             for tag in tags:
#                 if tag.strip():  # Only add non-empty tags
#                     product_tags_data.append({
#                         'product_id': product_id,
#                         'tag': tag.strip()
#                     })
        
#         # Create product_tags DataFrame
#         product_tags_df = pd.DataFrame(product_tags_data)
#         print(f"Extracted {len(product_tags_df)} product tag entries")
        
#         # Clean products data - remove tags column and fix timestamps
#         products_clean = products_df.drop(columns=['tags'], errors='ignore').copy()
#         products_clean['created_at'] = products_clean['created_at'].fillna(current_time)
#         products_clean['updated_at'] = products_clean['updated_at'].fillna(current_time)
        
#         # Save cleaned products data
#         products_clean.to_csv(f"{output_dir}/products_clean.csv", index=False)
#         product_tags_df.to_csv(f"{output_dir}/product_tags.csv", index=False)
        
#         print(f"✓ Cleaned products saved to: {output_dir}/products_clean.csv")
#         print(f"✓ Product tags saved to: {output_dir}/product_tags.csv")
        
#     except FileNotFoundError:
#         print("❌ Error: products_full_tesla.csv not found")
#         return False
#     except Exception as e:
#         print(f"❌ Error processing products: {e}")
#         return False
    
#     # ===== CLEAN MATERIALS DATA =====
#     print("\n2. Processing materials data...")
#     try:
#         materials_df = pd.read_csv("data/materials_tesla.csv")
#         print(f"Original materials data shape: {materials_df.shape}")
#         print(f"Original columns: {list(materials_df.columns)}")
        
#         # Extract tags before cleaning
#         material_tags_data = []
#         for _, row in materials_df.iterrows():
#             material_id = row['material_id']  # Use the first material_id column
#             tags = row['tags'].split(',') if pd.notna(row['tags']) else []
#             for tag in tags:
#                 if tag.strip():  # Only add non-empty tags
#                     material_tags_data.append({
#                         'material_id': material_id,
#                         'tag': tag.strip()
#                     })
        
#         # Create material_tags DataFrame
#         material_tags_df = pd.DataFrame(material_tags_data)
#         print(f"Extracted {len(material_tags_df)} material tag entries")
        
#         # Clean materials data - remove unnecessary columns
#         # Keep only the required columns for the materials table
#         materials_clean = materials_df.copy()
        
#         # Handle duplicate material_id column (keep the first one)
#         if 'material_id' in materials_clean.columns:
#             # Get the first material_id column
#             material_id_col = materials_clean['material_id']
            
#         # Remove columns that don't belong in materials table
#         columns_to_remove = ['tags', 'product_id']
#         materials_clean = materials_clean.drop(columns=columns_to_remove, errors='ignore')
        
#         # Handle duplicate material_id column if it exists
#         if list(materials_clean.columns).count('material_id') > 1:
#             # Keep only the first material_id column
#             materials_clean = materials_clean.loc[:, ~materials_clean.columns.duplicated()]
        
#         # Fix timestamps
#         materials_clean['created_at'] = materials_clean['created_at'].fillna(current_time)
#         materials_clean['updated_at'] = materials_clean['updated_at'].fillna(current_time)
        
#         # Save cleaned materials data
#         materials_clean.to_csv(f"{output_dir}/materials_clean.csv", index=False)
#         material_tags_df.to_csv(f"{output_dir}/material_tags.csv", index=False)
        
#         print(f"✓ Cleaned materials saved to: {output_dir}/materials_clean.csv")
#         print(f"✓ Material tags saved to: {output_dir}/material_tags.csv")
        
#     except FileNotFoundError:
#         print("❌ Error: materials_tesla.csv not found")
#         return False
#     except Exception as e:
#         print(f"❌ Error processing materials: {e}")
#         return False
    
#     # ===== CREATE BILL OF MATERIALS DATA =====
#     print("\n3. Processing Bill of Materials data...")
#     try:
#         # Read the original materials file for BOM data
#         materials_full_df = pd.read_csv("data/materials_tesla.csv")
        
#         # Create BOM entries from materials data
#         bom_data = []
#         for _, row in materials_full_df.iterrows():
#             if pd.notna(row['product_id']) and pd.notna(row['material_id']):
#                 bom_data.append({
#                     'product_id': row['product_id'],
#                     'material_id': row['material_id'],
#                     'subassembly_id': None,  # NULL for material entries
#                     'quantity': row['mass'],
#                     'unit': 'kg'
#                 })
        
#         bom_df = pd.DataFrame(bom_data)
#         bom_df.to_csv(f"{output_dir}/bill_of_materials.csv", index=False)
        
#         print(f"✓ Bill of Materials saved to: {output_dir}/bill_of_materials.csv")
#         print(f"  Created {len(bom_df)} BOM entries")
        
#     except Exception as e:
#         print(f"❌ Error creating BOM data: {e}")
    
#     # ===== SUMMARY =====
#     print("\n" + "="*50)
#     print("DATA CLEANUP SUMMARY")
#     print("="*50)
#     print(f"✓ Products cleaned: {len(products_clean)} records")
#     print(f"✓ Product tags extracted: {len(product_tags_df)} records")
#     print(f"✓ Materials cleaned: {len(materials_clean)} records")
#     print(f"✓ Material tags extracted: {len(material_tags_df)} records")
#     print(f"✓ BOM entries created: {len(bom_df)} records")
#     print(f"\nAll cleaned files saved to: {output_dir}/")
    
#     return True

# def validate_schema_compatibility():
#     """
#     Validate that the cleaned data matches the expected schema
#     """
#     print("\n" + "="*50)
#     print("SCHEMA VALIDATION")
#     print("="*50)
    
#     # Expected schema columns
#     expected_schemas = {
#         'products': ['product_id', 'name', 'category', 'price', 'stock', 'supplier', 'created_at', 'updated_at'],
#         'materials': ['material_id', 'material_name', 'material_type', 'mass', 'created_at', 'updated_at'],
#         'product_tags': ['product_id', 'tag'],
#         'material_tags': ['material_id', 'tag'],
#         'bill_of_materials': ['product_id', 'material_id', 'subassembly_id', 'quantity', 'unit']
#     }
    
#     output_dir = "data_cleaned"
    
#     for table_name, expected_cols in expected_schemas.items():
#         try:
#             if table_name == 'products':
#                 df = pd.read_csv(f"{output_dir}/products_clean.csv")
#             elif table_name == 'materials':
#                 df = pd.read_csv(f"{output_dir}/materials_clean.csv")
#             elif table_name == 'product_tags':
#                 df = pd.read_csv(f"{output_dir}/product_tags.csv")
#             elif table_name == 'material_tags':
#                 df = pd.read_csv(f"{output_dir}/material_tags.csv")
#             elif table_name == 'bill_of_materials':
#                 df = pd.read_csv(f"{output_dir}/bill_of_materials.csv")
            
#             actual_cols = list(df.columns)
            
#             print(f"\n{table_name.upper()}:")
#             print(f"  Expected columns: {expected_cols}")
#             print(f"  Actual columns:   {actual_cols}")
            
#             missing_cols = set(expected_cols) - set(actual_cols)
#             extra_cols = set(actual_cols) - set(expected_cols)
            
#             if missing_cols:
#                 print(f"  ❌ Missing columns: {missing_cols}")
#             if extra_cols:
#                 print(f"  ⚠️  Extra columns: {extra_cols}")
#             if not missing_cols and not extra_cols:
#                 print(f"  ✓ Schema matches perfectly!")
                
#         except FileNotFoundError:
#             print(f"  ❌ File not found for {table_name}")
#         except Exception as e:
#             print(f"  ❌ Error validating {table_name}: {e}")

# def show_sample_data():
#     """
#     Show sample data from cleaned files
#     """
#     print("\n" + "="*50)
#     print("SAMPLE DATA PREVIEW")
#     print("="*50)
    
#     output_dir = "data_cleaned"
#     files = [
#         ('products_clean.csv', 'PRODUCTS'),
#         ('materials_clean.csv', 'MATERIALS'),
#         ('product_tags.csv', 'PRODUCT TAGS'),
#         ('material_tags.csv', 'MATERIAL TAGS'),
#         ('bill_of_materials.csv', 'BILL OF MATERIALS')
#     ]
    
#     for filename, title in files:
#         try:
#             df = pd.read_csv(f"{output_dir}/{filename}")
#             print(f"\n{title} (First 3 rows):")
#             print(df.head(3).to_string(index=False))
#             print(f"Total rows: {len(df)}")
#         except FileNotFoundError:
#             print(f"\n{title}: File not found")
#         except Exception as e:
#             print(f"\n{title}: Error reading file - {e}")

# if __name__ == "__main__":
#     print("Tesla Database CSV Cleanup and Tag Separation")
#     print("=" * 50)
    
#     # Run the cleanup process
#     success = clean_and_separate_data()
    
#     if success:
#         # Validate the results
#         validate_schema_compatibility()
        
#         # Show sample data
#         show_sample_data()
        
#         print("\n" + "="*50)
#         print("NEXT STEPS:")
#         print("="*50)
#         print("1. Review the cleaned files in the 'data_cleaned' directory")
#         print("2. Update your database.py to use the cleaned CSV files")
#         print("3. Replace the original CSV files with the cleaned versions")
#         print("4. Run your database setup script with the cleaned data")
        
#     else:
#         print("\n❌ Cleanup process failed. Please check the error messages above.")