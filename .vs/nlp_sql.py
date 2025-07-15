import sqlite3
import requests
import os
import re
import pandas as pd
from dotenv import load_dotenv
from typing import Dict, List, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"
MODEL = "meta-llama/llama-4-scout-17b-16e-instruct"
DB_PATH = "company.db"

# Cache for schema to avoid repeated queries
_schema_cache: Dict[str, str] = {}

def generate_dynamic_schema(db_path: str = DB_PATH) -> str:
    """
    Query the database to generate a dynamic schema description.
    """
    if db_path in _schema_cache:
        return _schema_cache[db_path]

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        
        schema_description = []
        for table in tables:
            cursor.execute(f"PRAGMA table_info({table});")
            columns = cursor.fetchall()
            
            cursor.execute(f"PRAGMA foreign_key_list({table});")
            foreign_keys = cursor.fetchall()
            
            schema_description.append(f"Table: {table}")
            for col in columns:
                col_name, col_type, not_null, default_value, pk = col[1], col[2], col[3], col[4], col[5]
                col_desc = f"- {col_name} ({col_type}"
                if not_null:
                    col_desc += ", NOT NULL"
                if pk:
                    col_desc += ", PRIMARY KEY"
                col_desc += ")"
                schema_description.append(col_desc)
            
            if foreign_keys:
                for fk in foreign_keys:
                    fk_desc = f"- FOREIGN KEY ({fk[3]}) REFERENCES {fk[2]}({fk[4]})"
                    schema_description.append(fk_desc)
            
            schema_description.append("")
        
        conn.close()
        schema_text = "\n".join(schema_description) if schema_description else "No schema information available"
        _schema_cache[db_path] = schema_text
        return schema_text
    except Exception as e:
        logger.error(f"Error generating schema: {e}")
        return "Error: Unable to retrieve schema information"

def extract_table_aliases(sql: str) -> Dict[str, str]:
    """
    Extract table aliases from the SQL query's FROM and JOIN clauses.
    """
    aliases = {}
    pattern = r'\b(FROM|JOIN)\s+(\w+)\s*(?:AS\s+)?(\w+)?\b'
    matches = re.finditer(pattern, sql, re.IGNORECASE)
    for match in matches:
        table_name = match.group(2)
        alias = match.group(3) if match.group(3) else table_name[0].lower()
        aliases[alias] = table_name
    return aliases

def extract_cte_aliases(sql: str) -> set:
    """
    Extract CTE aliases from the WITH clause.
    """
    cte_aliases = set()
    with_match = re.match(r'\bWITH\s+(.+?)\s*(?:\bFROM\b|\bSELECT\b|$)', sql, re.IGNORECASE | re.DOTALL)
    if with_match:
        cte_text = with_match.group(1)
        cte_pattern = r'\b(\w+)\s+AS\s*\('
        matches = re.finditer(cte_pattern, cte_text, re.IGNORECASE)
        for match in matches:
            cte_aliases.add(match.group(1))
    return cte_aliases

def extract_select_aliases(sql: str) -> Dict[str, str]:
    """
    Extract column aliases defined in the SELECT clause.
    """
    aliases = {}
    select_match = re.search(r'\bSELECT\s+(.+?)\s*(?:\bFROM\b|\bWHERE\b|\bGROUP\b|\bORDER\b|\bLIMIT\b|$)', sql, re.IGNORECASE | re.DOTALL)
    if select_match:
        select_text = select_match.group(1)
        tokens = re.split(r',\s*(?![^()]*\))', select_text)
        for token in tokens:
            token = token.strip()
            as_match = re.search(r'\bAS\s+(\w+)\b', token, re.IGNORECASE)
            if as_match:
                alias = as_match.group(1)
                expr = token[:as_match.start()].strip()
                aliases[alias] = expr
    return aliases

def extract_cte_columns(sql: str, cte_aliases: set) -> Dict[str, List[str]]:
    """
    Extract columns defined in each CTE by parsing their SELECT clauses.
    """
    cte_columns = {alias: [] for alias in cte_aliases}
    with_match = re.match(r'\bWITH\s+(.+?)\s*(?:\bFROM\b|\bSELECT\b|$)', sql, re.IGNORECASE | re.DOTALL)
    if with_match:
        cte_text = with_match.group(1)
        cte_definitions = re.split(r',\s*(?=\w+\s+AS\s*\()', cte_text)
        for cte_def in cte_definitions:
            cte_name_match = re.match(r'\b(\w+)\s+AS\s*\(\s*SELECT\b', cte_def, re.IGNORECASE)
            if cte_name_match:
                cte_name = cte_name_match.group(1)
                if cte_name in cte_aliases:
                    select_clause = re.search(r'SELECT\s+(.+?)\s*(?:\bFROM\b|\))', cte_def, re.IGNORECASE | re.DOTALL)
                    if select_clause:
                        select_text = select_clause.group(1)
                        tokens = re.split(r',\s*(?![^()]*\))', select_text)
                        for token in tokens:
                            token = token.strip()
                            as_match = re.search(r'\bAS\s+(\w+)\b', token, re.IGNORECASE)
                            if as_match:
                                cte_columns[cte_name].append(as_match.group(1))
                            elif re.match(r'\w+\.\w+', token):
                                cte_columns[cte_name].append(token.split('.')[-1])
                            elif token not in ('*',) and not re.match(r'\w+\(', token):
                                cte_columns[cte_name].append(token)
    return cte_columns

def validate_columns(sql, schema_tables, cte_columns):
    """
    Validates that all columns in the SQL query exist in schema tables, CTEs, or as aliases.
    
    Args:
        sql (str): The SQL query to validate.
        schema_tables (dict): Dictionary of table names to their column lists.
        cte_columns (dict): Dictionary of CTE names to their column lists.
    
    Returns:
        bool: True if all columns are valid, False otherwise.
        list: List of error messages for invalid columns.
    """
    errors = []
    
    # Normalize SQL for consistent parsing
    sql = sql.strip()
    
    # Split SQL into clauses based on major SQL keywords
    clauses = re.split(r'\b(FROM|WHERE|GROUP BY|HAVING|ORDER BY|LIMIT)\b', sql, flags=re.IGNORECASE)
    current_tables = set()  # Track tables/CTEs referenced in FROM/JOIN
    
    for i in range(0, len(clauses), 2):
        clause_content = clauses[i].strip()
        clause_type = clauses[i + 1].upper() if i + 1 < len(clauses) else None
        
        # Handle FROM clauses separately
        if clause_type == 'FROM':
            # Extract table/CTE names from FROM and JOIN clauses
            from_clause = clause_content
            if i > 0:  # Include preceding SELECT clause for context
                from_clause = clauses[i - 2].strip() + ' ' + from_clause
            
            # Match table/CTE names in FROM and JOIN
            table_pattern = r'\bFROM\b\s+([^\s,]+)(?:\s+AS\s+\w+)?|\bJOIN\b\s+([^\s,]+)(?:\s+AS\s+\w+)?'
            for match in re.finditer(table_pattern, from_clause, re.IGNORECASE):
                table_name = match.group(1) or match.group(2)
                if table_name and (table_name in schema_tables or table_name in cte_columns):
                    current_tables.add(table_name)
                else:
                    errors.append(f"Table or CTE {table_name} not found in schema or CTEs")
            continue  # Skip column validation for FROM clauses
        
        # Process non-FROM clauses (SELECT, WHERE, GROUP BY, etc.)
        if clause_content:
            # Extract columns, excluding function calls and qualified names without table
            column_pattern = r'\b(?:(\w+)\.)?(\w+)\b(?!\s*\()'
            for match in re.finditer(column_pattern, clause_content):
                table_prefix, column = match.group(1), match.group(2)
                
                # Skip if column is a reserved keyword or part of a CTE name
                if column in cte_columns or column in schema_tables:
                    continue
                
                # Check if column is valid
                found = False
                if table_prefix:
                    # Qualified column (e.g., s.customer_id)
                    if table_prefix in schema_tables and column in schema_tables[table_prefix]:
                        found = True
                    elif table_prefix in cte_columns and column in cte_columns[table_prefix]:
                        found = True
                    else:
                        errors.append(f"Column {table_prefix}.{column} not found in referenced table or CTE")
                else:
                    # Unqualified column (e.g., total_revenue)
                    # Check all current tables/CTEs
                    for table in current_tables:
                        if (table in schema_tables and column in schema_tables[table]) or \
                           (table in cte_columns and column in cte_columns[table]):
                            found = True
                            break
                    # Check if column is an alias in the SELECT clause
                    if not found and i == 0:  # SELECT clause
                        alias_pattern = r'\b\w+\s+AS\s+' + re.escape(column) + r'\b'
                        if re.search(alias_pattern, clause_content, re.IGNORECASE):
                            found = True
                    if not found:
                        errors.append(f"Column {column} not found in any referenced table or CTE")
    
    return len(errors) == 0, errors

def clean_sql_output(raw_sql: str) -> str:
    """
    Enhanced SQL cleaning function to preserve CTEs and extract valid SQL queries.
    """
    cleaned = re.sub(r"```sql|```", "", raw_sql, flags=re.IGNORECASE)
    cleaned = re.sub(r"--.*", "", cleaned)
    cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
    cleaned = ' '.join(cleaned.split())
    
    select_pattern = r'(WITH\s+.*?|SELECT\s+.*?)(?:;|$|\n\n)'
    match = re.search(select_pattern, cleaned, re.IGNORECASE | re.DOTALL)
    if not match:
        logger.warning("No valid SELECT or WITH query found in raw SQL")
        return "SELECT 'Invalid query' as error_message;"
    
    result = match.group(1).strip()
    result = result.rstrip(';').strip() + ';'
    result = re.sub(r'\b(\w+)\.(\w+)\.(\w+)\b', r'\1.\3', result)
    
    return result

def enforce_column_qualification(sql: str, schema: str) -> str:
    """
    Qualify columns with table aliases based on the query's FROM/JOIN clauses and schema.
    """
    aliases = extract_table_aliases(sql)
    if not aliases:
        logger.warning("No table aliases found in SQL query")
        return sql

    table_columns = {}
    current_table = None
    for line in schema.split('\n'):
        if line.startswith('Table: '):
            current_table = line.replace('Table: ', '').strip()
            table_columns[current_table] = []
        elif line.startswith('- ') and current_table and '(' in line:
            col_name = line.split('(')[0].replace('- ', '').strip()
            table_columns[current_table].append(col_name)
    
    def replace_column(match):
        alias_or_none, col = match.group(1), match.group(2)
        if alias_or_none:
            return match.group(0)
        possible_tables = [t for t, cols in table_columns.items() if col in cols]
        if len(possible_tables) == 1:
            for alias, table in aliases.items():
                if table == possible_tables[0]:
                    return f"{alias}.{col}"
        elif len(possible_tables) > 1:
            logger.warning(f"Ambiguous column {col} in tables {possible_tables}")
        return col
    
    sql = re.sub(r'\b(\w+\.)?(\w+)\b(?!\s*\()', replace_column, sql)
    return sql

def text_to_sql(nl_query: str, db_path: str = DB_PATH) -> str:
    """
    Uses Groq API to convert a natural language query to SQL, using a dynamically generated schema.
    """
    schema = generate_dynamic_schema(db_path)
    
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }

    system_prompt = f"""
You are a SQL generation engine for an internal SQLite business database. Generate precise SQL SELECT queries to answer questions about internal company data based on the provided schema. For production capacity or bottleneck queries, calculate maximum units based on component availability and identify limiting factors.

### Database Schema:
{schema}

### IMPORTANT INSTRUCTIONS:

1. **Output Format**: Return ONLY the executable SQL SELECT query. No explanations, no markdown, no comments, no text before or after the SQL.

2. **Focus on Internal Data Only**: Only generate SQL for internal company data. Ignore external data (e.g., stock prices, news).

3. **Production Capacity Queries**:
   - For queries about manufacturing capacity (e.g., "maximum units"), identify tables with inventory or stock data and select quantities for relevant components (e.g., products with specific tags or categories).
   - Assume one unit of each critical component is needed per product unless specified.
   - For bottleneck analysis, identify the component with the minimum quantity using MIN(quantity) or ORDER BY quantity ASC LIMIT 1.

4. **JOIN Rules**:
   - Always use explicit JOINs with correct foreign keys.
   - Use table aliases (first letter of table name, e.g., p for products, i for inventory).
   - Always qualify columns with aliases (e.g., p.name, i.quantity).
   - Avoid nested or invalid aliases (e.g., p.w.name).

5. **Fuzzy Matching**:
   - Use LOWER(column) LIKE '%keyword%' for partial matches on names, tags, or categories.
   - For brand-specific queries, check name, tags, or category columns.
   - For component-specific queries (e.g., "battery and motor"), generate AND conditions for multiple criteria.

6. **Bottleneck Analysis**:
   - For queries asking about bottlenecks, select the component with the lowest quantity in relevant tables.
   - Use ORDER BY quantity ASC LIMIT 1 or MIN(quantity).

7. **Complex Queries**:
   - For queries requiring comparisons (e.g., above-average revenue), use subqueries or CTEs to compute aggregates.
   - For percentage calculations, use subqueries or CTEs to compute totals and ensure floating-point division (e.g., multiply by 1.0).
   - Ensure CTEs are properly structured with WITH clauses.
   - Avoid redundant joins in CTEs; reference prior CTEs where possible.

8. **Examples**:
   - "Show all products" → SELECT p.product_id, p.name, p.category, p.price, p.stock, p.supplier, p.tags FROM products p;
   - "Show sales for Apple products" → SELECT p.name, s.quantity, s.total_amount FROM sales s JOIN products p ON s.product_id = p.product_id WHERE LOWER(p.name) LIKE '%apple%' OR LOWER(p.tags) LIKE '%apple%';
   - "Show daily revenue for the last 30 days" → SELECT DATE(s.date) AS date, SUM(s.total_amount) AS daily_revenue FROM sales s WHERE s.date >= DATE('2025-01-01') GROUP BY DATE(s.date) ORDER BY date DESC;
   - "Which products have low stock?" → SELECT p.product_id, p.name, p.category, p.price, p.stock, p.supplier, p.tags FROM products p WHERE p.stock < 100 ORDER BY p.stock ASC;
   - "Maximum units we can produce" → SELECT MIN(i.quantity) AS max_units FROM inventory i JOIN products p ON i.product_id = p.product_id WHERE LOWER(p.tags) LIKE '%component%';
   - "Primary bottleneck" → SELECT p.name, i.quantity, p.tags FROM inventory i JOIN products p ON i.product_id = p.product_id WHERE LOWER(p.tags) LIKE '%component%' ORDER BY i.quantity ASC LIMIT 1;
   - "Show battery materials stock levels" → SELECT m.material_name, m.mass, i.quantity FROM materials m JOIN inventory i ON m.product_id = i.product_id WHERE LOWER(m.tags) LIKE '%battery%';
   - "Show products with above-average revenue in their category" → WITH product_sales AS (SELECT p.product_id, p.name, p.category, SUM(s.total_amount) AS total_revenue, SUM(s.quantity) AS total_quantity FROM sales s JOIN products p ON s.product_id = p.product_id WHERE s.date >= DATE('2025-01-01') GROUP BY p.product_id, p.name, p.category), category_avg AS (SELECT category, AVG(total_revenue) AS avg_revenue FROM product_sales GROUP BY category), category_total AS (SELECT category, SUM(total_revenue) AS total_category_revenue FROM product_sales GROUP BY category) SELECT ps.product_id, ps.name, ps.category, ps.total_revenue, ps.total_quantity, ROUND(ps.total_revenue * 1.0 / ct.total_category_revenue * 100, 2) AS revenue_percentage FROM product_sales ps JOIN category_avg ca ON ps.category = ca.category JOIN category_total ct ON ps.category = ct.category WHERE ps.total_revenue > ca.avg_revenue ORDER BY ps.category, ps.total_revenue DESC;

9. **Regional Analysis**: Use columns like region or location for customer or sales analysis.
10. **Time Analysis**: Use date or timestamp columns for time-based analysis.
11. **Aggregation**: Use SUM, COUNT, MIN, MAX for summary statistics when appropriate.

12. **Error Handling**: If the query is unclear or cannot be translated, return: SELECT 'Invalid query' as error_message;

CRITICAL: Return only the SQL query, nothing else!
"""

    user_prompt = f"Generate SQL for the internal database part of this query: {nl_query}"

    data = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.1
    }

    try:
        response = requests.post(GROQ_API_URL, headers=headers, json=data)
        response.raise_for_status()
        result = response.json()
        raw_sql = result["choices"][0]["message"]["content"]
        logger.info(f"Raw SQL from Groq: {raw_sql}")
        return raw_sql
    except Exception as e:
        logger.error(f"Error in text_to_sql: {e}")
        logger.error(f"Full response: {result if 'result' in locals() else 'No response'}")
        return "SELECT 'Invalid query' as error_message;"

def run_nl_query(nl_query: str, db_path: str = DB_PATH):
    """
    Convert natural language query to SQL and execute it.
    """
    logger.info(f"Processing query: {nl_query}")
    raw_sql = text_to_sql(nl_query, db_path)
    logger.info(f"Raw SQL Generated:\n{raw_sql}")

    # Post-process SQL to enforce correct thresholds/date ranges
    if "low stock" in nl_query.lower():
        raw_sql = raw_sql.replace("p.stock < 10", "p.stock < 100")
    if "daily revenue" in nl_query.lower():
        raw_sql = raw_sql.replace("DATE('now', '-30 days')", "DATE('2025-01-01')")
    if "2025" in nl_query.lower():
        raw_sql = raw_sql.replace("STRFTIME('%Y', s.date) = '2025'", "s.date >= DATE('2025-01-01')")

    schema = generate_dynamic_schema(db_path)
    cleaned_sql = clean_sql_output(raw_sql)
    logger.info(f"Cleaned SQL:\n{cleaned_sql}")

    aliases = extract_table_aliases(cleaned_sql)
    is_valid, validation_errors = validate_columns(cleaned_sql, schema, aliases)
    if not is_valid:
        logger.error(f"SQL validation errors: {validation_errors}")
        return {
            "success": False,
            "message": f"Invalid SQL query: {', '.join(validation_errors)}",
            "suggestions": ["Try rephrasing your question", "Check if the data exists in the system"],
            "query": nl_query,
            "query_type": "simple",
            "display_format": "table",
            "summary": "Validation failed"
        }

    fixed_sql = enforce_column_qualification(cleaned_sql, schema)
    logger.info(f"Final SQL Query:\n{fixed_sql}")

    if not fixed_sql or not any(keyword in fixed_sql.upper() for keyword in ['SELECT', 'WITH']):
        logger.error("No valid SELECT or WITH query generated")
        return {
            "success": False,
            "message": "No valid SQL query could be generated from the input",
            "suggestions": ["Try rephrasing your question", "Check if the data exists in the system"],
            "query": nl_query,
            "query_type": "simple",
            "display_format": "table",
            "summary": "No valid query"
        }

    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(fixed_sql, conn)
        conn.close()
        
        logger.info(f"Query returned {len(df)} rows")
        if len(df) > 0:
            logger.info("Sample of returned data:")
            logger.info(df.head().to_string())
            return {
                "success": True,
                "data": df.to_dict(orient="records"),
                "count": len(df),
                "query": nl_query,
                "columns": df.columns.tolist(),
                "query_type": "simple",
                "display_format": "table",
                "summary": f"Found {len(df)} results"
            }
        else:
            logger.warning("No rows returned from query")
            message = "No data found for your query"
            if "stock < 100" in fixed_sql:
                message = "No products have stock levels below 100; try a higher threshold"
            elif "material_name LIKE '%battery%'" in fixed_sql:
                message = "No materials with 'battery' in their name found; try filtering by tags"
            elif "DATE('2025-01-01')" in fixed_sql:
                message = "No sales data found for 2025; try a different date range"
            return {
                "success": False,
                "message": message,
                "suggestions": ["Try rephrasing your question", "Check if the data exists in the system"],
                "query": nl_query,
                "query_type": "simple",
                "display_format": "table",
                "summary": "No data found"
            }
        
    except Exception as e:
        logger.error(f"Execution Error: {e}")
        return {
            "success": False,
            "message": f"Execution failed on sql '{fixed_sql}': {str(e)}",
            "suggestions": ["Try rephrasing your question", "Check if the data exists in the system"],
            "query": nl_query,
            "query_type": "simple",
            "display_format": "table",
            "summary": "Execution failed"
        }