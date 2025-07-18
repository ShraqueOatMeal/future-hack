# from business_analyzer import route_query_optimized
# from nlp_sql import generate_dynamic_schema
# import json

# def run_test(db_path, queries):
#     print(f"\nTesting database: {db_path}")
#     print("Schema:\n", generate_dynamic_schema(db_path))
    
#     for query in queries:
#         print(f"\nQuery: {query}")
#         result = route_query_optimized(query)
#         print("Result:\n", json.dumps(result, indent=2))

# if __name__ == "__main__":
#     # Test queries for company.db
#     queries = [
        
#         "Show the top 5 regions by total revenue for EV products in 2025, including the number of unique customers and average sale amount per transaction, only for sales where the product stock is below 100",
#         "Show products with sales in 2025 where the total revenue exceeds the average revenue of all products in their category, including the total quantity sold and the percentage of total revenue for that category"
#     ]
#     run_test("company.db", queries)


from nlp_sql import run_nl_query
import sqlite3

# Print schema for debugging
# schema = generate_dynamic_schema("company.db")
# result1 = run_nl_query("Show products and their sales in 2025")


 # # Test Query 1
 # result1 = run_nl_query("Show products with sales in 2025 where the total revenue exceeds the average revenue of all products in their category, including the total quantity sold and the percentage of total revenue for that category")
 # print("Result for Query 1:")
 # print(result1)
def test_direct_sql():
    conn = sqlite3.connect("company.db")
    cursor = conn.cursor()
    
    # Test Query 1: EV products revenue by region
    query1 = """
    SELECT sql FROM sqlite_master WHERE name='products_fts';
    """
    query2 = """
    SELECT p.name, p.category, p.price
FROM products p
JOIN products_fts ON products_fts.rowid = p.product_id
WHERE products_fts MATCH 'Model S*';
    """
    query3 = """SELECT pt.product_id, pt.tag
FROM product_tags pt
WHERE pt.product_id IN (122, 133) AND pt.tag = 'EV';"""
    query4 = """SELECT sql FROM sqlite_master WHERE name='products_fts';"""
    query5 = """SELECT rowid, name FROM products_fts LIMIT 5;"""


    # Execute tests
    queries = [
        ("Tables", query1),
        ("full text search", query2),
        ("bill of materials", query3),
        # ("products_fts schema", query4),
        # ("Sample products_fts data", query5)
        # ("Low Stock High Demand Products", query4)
    ]
    
    for test_name, query in queries:
        print(f"\n=== {test_name} ===")
        try:
            cursor.execute(query)
            results = cursor.fetchall()
            for row in results:
                print(row)
        except Exception as e:
            print(f"Error: {e}")
    
    conn.close()

if __name__ == "__main__":
    test_direct_sql()
