import sqlite3
import pandas as pd

DB_NAME = "company.db"

def get_max_production_and_bottleneck():
    conn = sqlite3.connect(DB_NAME)
    query = """
    SELECT b.product_id, p.name AS product_name,
           b.material_name, b.quantity_required_per_unit, b.scrap_rate_percent,
           m.quantity AS available_quantity
    FROM bom b
    JOIN products p ON b.product_id = p.product_id
    JOIN materials m ON b.material_name = m.material_name
    """
    df = pd.read_sql_query(query, conn)
    conn.close()

    # Calculate the effective needed quantity after scrap losses
    df['effective_needed'] = df['quantity_required_per_unit'] * (1 + df['scrap_rate_percent']/100)

    # Calculate max units producible by each material
    df['max_units_by_material'] = df['available_quantity'] / df['effective_needed']

    # Get production limit (minimum across all materials for each product)
    min_limits = df.groupby(['product_id', 'product_name'])['max_units_by_material'].min().reset_index()
    min_limits = min_limits.rename(columns={'max_units_by_material': 'max_possible_units'})

    # Find the bottleneck component
    bottlenecks_idx = df.groupby(['product_id'])['max_units_by_material'].idxmin()
    bottlenecks = df.loc[bottlenecks_idx, ['product_id', 'material_name']].reset_index(drop=True)

    # Merge into final table
    result = pd.merge(min_limits, bottlenecks, on='product_id')

    return result

if __name__ == "__main__":
    summary = get_max_production_and_bottleneck()
    print(summary.to_markdown(index=False))
