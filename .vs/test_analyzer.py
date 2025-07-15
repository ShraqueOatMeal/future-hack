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
    WITH ev_sales AS (
      SELECT s.date, s.customer_id, s.total_amount, c.region, p.stock
      FROM sales s
      JOIN products p ON s.product_id = p.product_id
      JOIN customers c ON s.customer_id = c.customer_id
      WHERE LOWER(p.tags) LIKE '%ev%' AND s.date >= DATE('2025-01-01') AND p.stock < 100
    ),
    region_revenue AS (
      SELECT region, SUM(total_amount) AS total_revenue, COUNT(DISTINCT customer_id) AS unique_customers, AVG(total_amount) AS avg_sale_amount
      FROM ev_sales
      GROUP BY region
    )
    SELECT region, total_revenue, unique_customers, avg_sale_amount
    FROM region_revenue
    ORDER BY total_revenue DESC
    LIMIT 5;
    """
    

    
    # Execute tests
    queries = [
        ("EV Revenue by Region", query1),
        # ("Products Above Category Average", query2),
        # ("Top Customers by Frequency", query3),
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
