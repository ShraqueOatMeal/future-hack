from encodings.aliases import aliases
import json
import os
import sqlite3
import pandas as pd
from dotenv import load_dotenv
import re
from typing import Dict, List, Tuple
import logging
from openai import OpenAI

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

load_dotenv()
XAI_API_KEY = os.getenv("XAI_API_KEY")
if not XAI_API_KEY:
    raise ValueError("XAI_API_KEY not found")
GROQ_API_URL = "https://api.x.ai/v1"
MODEL = "grok-3"
DB_PATH = "company.db"

client = OpenAI(api_key=XAI_API_KEY, base_url=GROQ_API_URL)

# Cache for schema and query results
_schema_cache: Dict[str, str] = {}
_query_cache: Dict[str, dict] = {}

def check_fts_table(db_path: str = DB_PATH) -> bool:
    """Check if products_fts table exists and is properly configured as an FTS5 table."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='products_fts';")
        table_exists = cursor.fetchone()
        if not table_exists:
            logger.error("products_fts table does not exist")
            return False
        # Verify FTS5 configuration
        cursor.execute("SELECT sql FROM sqlite_master WHERE name='products_fts';")
        sql = cursor.fetchone()[0]
        if 'USING FTS5' not in sql.upper():
            logger.error("products_fts is not an FTS5 table")
            return False
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error checking products_fts: {e}")
        return False

def populate_fts_table(db_path: str = DB_PATH):
    """Populate products_fts table from products table if empty or missing."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        if not check_fts_table(db_path):
            logger.info("Creating products_fts table")
            cursor.execute("CREATE VIRTUAL TABLE IF NOT EXISTS products_fts USING FTS5(name, content='products', content_rowid='product_id');")
            cursor.execute("INSERT OR REPLACE INTO products_fts(rowid, name) SELECT product_id, name FROM products;")
            conn.commit()
        else:
            # Check if products_fts is empty
            cursor.execute("SELECT COUNT(*) FROM products_fts;")
            count = cursor.fetchone()[0]
            if count == 0:
                logger.info("Populating empty products_fts table")
                cursor.execute("INSERT OR REPLACE INTO products_fts(rowid, name) SELECT product_id, name FROM products;")
                conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error populating products_fts: {e}")

def generate_dynamic_schema(db_path: str = DB_PATH) -> str:
    if db_path in _schema_cache:
        return _schema_cache[db_path]

    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT name FROM sqlite_master WHERE type IN ('table', 'view');")
        tables = [row[0] for row in cursor.fetchall()]
        
        schema_description = []
        for table in tables:
            if table.startswith('products_fts_') and table != 'products_fts':
                continue
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
        
        # Add products_fts description
        if check_fts_table(db_path):
            schema_description.append("Table: products_fts (Virtual FTS Table)")
            schema_description.append("- rowid (INTEGER, links to products.product_id)")
            schema_description.append("- name (TEXT, searchable via MATCH)")
            schema_description.append("- Note: Use MATCH for full-text searches, e.g., WHERE products_fts MATCH 'keyword*';")
            schema_description.append("")
        
        conn.close()
        schema_text = "\n".join(schema_description) if schema_description else "No schema information available"
        _schema_cache[db_path] = schema_text
        return schema_text
    except Exception as e:
        logger.error(f"Error generating schema: {e}")
        return "Error: Unable to retrieve schema information"

def extract_table_aliases(sql: str) -> Dict[str, str]:
    aliases = {}
    pattern = r'\b(FROM|JOIN)\s+(\w+)\s*(?:AS\s+)?(\w+)?\b'
    matches = re.finditer(pattern, sql, re.IGNORECASE)
    for match in matches:
        table_name = match.group(2)
        alias = match.group(3) if match.group(3) else table_name[0].lower()
        aliases[alias] = table_name
    return aliases

def extract_cte_aliases(sql: str) -> set:
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
    cte_columns = {alias: [] for alias in cte_aliases}
    with_match = re.search(r'\bWITH\s+(.+?)\s*(SELECT|INSERT|UPDATE|DELETE|\Z)', sql, re.IGNORECASE | re.DOTALL)
    if with_match:
        cte_text = with_match.group(1)
        cte_definitions = re.split(r',\s*(?=\w+\s+AS\s*\()', cte_text)
        for cte_def in cte_definitions:
            cte_name_match = re.match(r'\b(\w+)\s+AS\s*\(\s*SELECT\b', cte_def, re.IGNORECASE)
            if not cte_name_match:
                continue

            cte_name = cte_name_match.group(1)
            if cte_name not in cte_aliases:
                continue

            select_clause_match = re.search(r'SELECT\s+(.+?)\s+FROM\s', cte_def, re.IGNORECASE | re.DOTALL)
            if not select_clause_match:
                continue

            select_text = select_clause_match.group(1)
            tokens = re.split(r',\s*(?![^()]*\))', select_text)

            for token in tokens:
                token = token.strip()
                as_match = re.search(r'\bAS\s+(\w+)', token, re.IGNORECASE)
                if as_match:
                    cte_columns[cte_name].append(as_match.group(1))
                elif '.' in token:
                    cte_columns[cte_name].append(token.split('.')[-1])
                elif token == '*':
                    # can't resolve without schema context, so assume wildcard and skip validation
                    cte_columns[cte_name].append('*')
                else:
                    cte_columns[cte_name].append(token.split()[-1])  # fallback for raw column
    return cte_columns


SQL_KEYWORDS = {
    'SELECT', 'FROM', 'JOIN', 'ON', 'WHERE', 'GROUP', 'BY', 'HAVING', 'ORDER', 'LIMIT',
    'LIKE', 'AS', 'AND', 'OR', 'NOT', 'IN', 'BETWEEN', 'SUM', 'COUNT', 'MIN', 'MAX',
    'AVG', 'DATE', 'LOWER', 'UPPER', 'WITH', 'ASC', 'DESC', 'MATCH'
}

FORBIDDEN_SQL_KEYWORDS = {
    'DELETE', 'DROP', 'TRUNCATE', 'ALTER', 'UPDATE', 'INSERT', 'REPLACE',
    'CREATE', 'GRANT', 'REVOKE', 'EXECUTE', 'CALL', 'MERGE', 'UPSERT',
    'LOAD', 'BULK', 'IMPORT', 'EXPORT', 'BACKUP', 'RESTORE', 'RENAME'
}

def validate_columns(sql: str, schema: str, cte_columns: Dict[str, List[str]]) -> Tuple[bool, List[str]]:
    errors = []
    
    schema_tables = {}
    current_table = None
    for line in schema.split('\n'):
        if line.startswith('Table: '):
            current_table = line.replace('Table: ', '').strip()
            schema_tables[current_table] = []
        elif line.startswith('- ') and current_table and '(' in line:
            col_name = line.split('(')[0].replace('- ', '').strip()
            schema_tables[current_table].append(col_name)
    
    schema_tables['products_fts'] = ['rowid', 'name']
    
    aliases = extract_table_aliases(sql)
    cte_aliases = extract_cte_aliases(sql)
    select_aliases = extract_select_aliases(sql)
    current_tables = set(aliases.values()) | cte_aliases
    alias_names = set(aliases.keys())
    
    literals = set()
    like_pattern = r"LIKE\s*['\"]([^'\"]*?)['\"]"
    match_pattern = r"MATCH\s*['\"]([^'\"]*?)['\"]"
    for pattern in [like_pattern, match_pattern]:
        for match in re.finditer(pattern, sql, re.IGNORECASE):
            literal = match.group(1).lower()
            words = re.findall(r'\w+', literal)
            literals.update(words)
    
    masked_sql = re.sub(r"'[^']*'", "QUOTED_STRING", sql)
    
    clauses = re.split(r'\b(FROM|WHERE|GROUP BY|HAVING|ORDER BY|LIMIT)\b', masked_sql, flags=re.IGNORECASE)
    
    for i in range(0, len(clauses), 2):
        clause_content = clauses[i].strip()
        clause_type = clauses[i + 1].upper() if i + 1 < len(clauses) else None
        
        if clause_type == 'FROM':
            continue
        
        if clause_content:
            column_pattern = r'\b(?:(\w+)\.)?([a-zA-Z_]\w*)\b(?!\s*\()'
            for match in re.finditer(column_pattern, clause_content):
                table_prefix, column = match.group(1), match.group(2)
                
                if (column.upper() in SQL_KEYWORDS or 
                    column in cte_aliases or 
                    column in schema_tables or 
                    column in alias_names or
                    column in select_aliases or
                    column.lower() in literals or
                    column == 'QUOTED_STRING'):
                    logger.debug(f"Skipping token {column} (keyword, alias, literal, or quoted string)")
                    continue
                
                found = False
                if table_prefix:
                    if table_prefix in alias_names:
                        actual_table = aliases.get(table_prefix, table_prefix)
                        if actual_table in schema_tables and column in schema_tables[actual_table]:
                            found = True
                        elif actual_table in cte_columns and column in cte_columns[actual_table]:
                            found = True
                        elif table_prefix in cte_columns and column in cte_columns[table_prefix]:
                            found = True  # alias directly matches CTE
                        elif '*' in cte_columns.get(actual_table, []) or '*' in cte_columns.get(table_prefix, []):
                            found = True  # wildcard column fallback
                        else:
                            errors.append(f"Column {table_prefix}.{column} not found in table {actual_table} or CTE {table_prefix}")
                    else:
                        errors.append(f"Table alias {table_prefix} not defined in query")

                else:
                    for table in current_tables:
                        if (table in schema_tables and column in schema_tables[table]) or \
                           (table in cte_columns and column in cte_columns[table]):
                            found = True
                            break
                    if not found and i == 0 and column in select_aliases:
                        found = True
                    if not found:
                        errors.append(f"Column {column} not found in any referenced table or CTE")
                
                logger.debug(f"Validated {table_prefix}.{column} {'successfully' if found else 'failed'}")
    
    errors = list(set(errors))
    logger.info(f"Column validation {'succeeded' if not errors else 'failed'}: {errors}")

    logger.debug("CTE columns: %s", json.dumps(cte_columns, indent=2))
    logger.debug("Table aliases: %s", aliases)
    logger.debug("Current tables: %s", current_tables)

    return len(errors) == 0, errors

def clean_sql_output(raw_sql: str) -> str:
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
    result = re.sub(r'\bfts\s+MATCH\b', 'products_fts MATCH', result, flags=re.IGNORECASE)
    
    match_pattern = r"MATCH\s+'([^']*)'"
    matches = re.findall(match_pattern, result, re.IGNORECASE)
    for match in matches:
        if any(kw.upper() in match.upper() for kw in FORBIDDEN_SQL_KEYWORDS):
            logger.error(f"Unsafe MATCH clause detected: {match}")
            return "SELECT 'Invalid query: unsafe MATCH clause' as error_message;"
    
    return result

def enforce_column_qualification(sql: str, schema: str) -> str:
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
        if alias_or_none or col.upper() in SQL_KEYWORDS:
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
    if "like" in nl_query.lower() and not check_fts_table(db_path):
        populate_fts_table(db_path)
    
    schema = generate_dynamic_schema(db_path)
    
    system_prompt = f"""
You are a SQL generation engine for an internal SQLite business database. Generate precise SQL SELECT queries to answer questions about internal company data based on the provided schema. For production capacity or bottleneck queries, calculate maximum units based on component availability and identify limiting factors.

### Database Schema:
{schema}

### IMPORTANT INSTRUCTIONS:

1. Output Format: Return ONLY the executable SQL SELECT query. No explanations, no markdown, no comments, no text before or after the SQL.

2. Table Aliases Required: Always assign *table aliases* (e.g., p for products, c for customers, s for sales, etc.). Use them *consistently throughout the query* — this includes SELECT, JOIN, WHERE, GROUP BY, and ORDER BY clauses.

3. Fully Qualify All Columns: *NEVER use unqualified column names*. Always prefix columns with the table alias (e.g., p.name, s.total_amount). This prevents ambiguity, especially in JOINs or subqueries.

4. Focus on Internal Data Only: Only generate SQL for internal company data. Ignore external data (e.g., stock prices, news).

5. Production Capacity Queries:
   - Identify inventory or stock-related tables.
   - Use MIN() to compute max producible units if components are limiting.
   - Example: 
     sql
     SELECT MIN(i.quantity) AS max_units 
     FROM inventory i 
     JOIN products p ON i.product_id = p.product_id 
     WHERE LOWER(p.tags) LIKE '%component%';
     

6. Fuzzy and Keyword Searches:
   - Use products_fts with MATCH for full-text queries.
   - Always join products_fts to products using products_fts.rowid = p.product_id.
   - Use wildcards like 'Model*' when appropriate.

7. JOIN Rules:
   - Always use *explicit JOINs* with correct foreign keys.
   - Always use and apply aliases like: p, s, c, i, w, m, b.
   - Always qualify all column names with aliases, including in conditions and aggregates.
   - Avoid implicit joins, ambiguous references, or nested alias chains like p.w.name.

8. Business Semantics Mapping:
   - "revenue", "income", "sales amount" → s.total_amount
   - "customer name" → c.customer_name
   - "product name" → p.product_name

9. Bottleneck Analysis:
   - Find the lowest i.quantity where p.tags contains 'component'.
   - Example:
     sql
     SELECT p.name, i.quantity, p.tags 
     FROM inventory i 
     JOIN products p ON i.product_id = p.product_id 
     WHERE LOWER(p.tags) LIKE '%component%' 
     ORDER BY i.quantity ASC 
     LIMIT 1;
     

10. Complex Queries:
    - Use CTEs (WITH ...) to calculate aggregates like averages, totals, or rankings.
    - Qualify every column in every SELECT/CTE with aliases.
    - Avoid ambiguous references and reuse of unaliased fields in joins.
    - Ensure subqueries and CTEs are readable, valid, and alias-safe.

11. Regional Analysis:
    - Always join customers (c) with sales (s) using customer_id.
    - Example:
      sql
      SELECT c.region, SUM(s.total_amount) AS total_sales 
      FROM sales s 
      JOIN customers c ON s.customer_id = c.customer_id 
      GROUP BY c.region;
      

12. Time Analysis:
    - Always apply DATE() if needed on time-based fields.
    - Example:
      sql
      SELECT DATE(s.date) AS date, SUM(s.total_amount) AS daily_revenue 
      FROM sales s 
      WHERE s.date >= DATE('2025-01-01') 
      GROUP BY DATE(s.date) 
      ORDER BY date DESC;
      

13. Aggregation:
    - Use SUM(), AVG(), COUNT(), MIN(), MAX() only with qualified column names.
    - All fields in GROUP BY must also be qualified with aliases.

14. Error Handling:
    - If query is unclear, return:
      sql
      SELECT 'Invalid query' AS error_message;
      

CRITICAL: All output must follow these alias and qualification rules strictly to prevent SQL errors. No comments, no markdown. Return only valid SQL SELECT queries.
"""

    user_prompt = f"Generate SQL for the internal database part of this query: {nl_query}"

    try:
        response = client.chat.completions.create(
            model=MODEL,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.1
        )
        raw_sql = response.choices[0].message.content
        logger.info(f"Raw SQL from Groq: {raw_sql}")
        return raw_sql
    except Exception as e:
        logger.error(f"Error in text_to_sql: {e}")
        return "SELECT 'Invalid query' as error_message;"

def run_nl_query(nl_query: str, db_path: str = DB_PATH):
    logger.info(f"Processing query: {nl_query}")
    cache_key = f"{nl_query}:{db_path}"
    if cache_key in _query_cache:
        logger.info(f"Returning cached result for query: {nl_query}")
        return _query_cache[cache_key]

    if "like" in nl_query.lower():
        populate_fts_table(db_path)

    raw_sql = text_to_sql(nl_query, db_path)
    logger.info(f"Raw SQL Generated:\n{raw_sql}")

    if "low stock" in nl_query.lower():
        raw_sql = raw_sql.replace("p.stock < 10", "p.stock < 100")
    if "daily revenue" in nl_query.lower():
        raw_sql = raw_sql.replace("DATE('now', '-30 days')", "DATE('2025-01-01')")
    if "2025" in nl_query.lower():
        raw_sql = raw_sql.replace("STRFTIME('%Y', s.date) = '2025'", "s.date >= DATE('2025-01-01')")

    schema = generate_dynamic_schema(db_path)
    cleaned_sql = clean_sql_output(raw_sql)
    logger.info(f"Cleaned SQL:\n{cleaned_sql}")

    cte_columns = extract_cte_columns(cleaned_sql, extract_cte_aliases(cleaned_sql))
    is_valid, validation_errors = validate_columns(cleaned_sql, schema, cte_columns)
    if not is_valid:
        logger.error(f"SQL validation errors: {validation_errors}")
        result = {
            "success": False,
            "message": f"Invalid SQL query: {', '.join(validation_errors)}",
            "suggestions": ["Try rephrasing your question", "Check if the data exists in the system"],
            "query": nl_query,
            "query_type": "simple",
            "display_format": "table",
            "summary": "Validation failed",
            "raw_sql": raw_sql,
            "final_sql": cleaned_sql
        }
        _query_cache[cache_key] = result
        return result

    fixed_sql = enforce_column_qualification(cleaned_sql, schema)
    logger.info(f"Final SQL Query:\n{fixed_sql}")

    if not fixed_sql or not any(keyword in fixed_sql.upper() for keyword in ['SELECT', 'WITH']):
        logger.error("No valid SELECT or WITH query generated")
        result = {
            "success": False,
            "message": "No valid SQL query could be generated from the input",
            "suggestions": ["Try rephrasing your question", "Check if the data exists in the system"],
            "query": nl_query,
            "query_type": "simple",
            "display_format": "table",
            "summary": "No valid query",
            "raw_sql": raw_sql,
            "final_sql": cleaned_sql
        }
        _query_cache[cache_key] = result
        return result

    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(fixed_sql, conn)
        conn.close()
        
        logger.info(f"Query returned {len(df)} rows")
        if len(df) > 0:
            logger.info("Sample of returned data:")
            logger.info(df.head().to_string())
            result = {
                "success": True,
                "data": df.to_dict(orient="records"),
                "count": len(df),
                "query": nl_query,
                "columns": df.columns.tolist(),
                "query_type": "simple",
                "display_format": "table",
                "summary": f"Found {len(df)} results",
                "raw_sql": raw_sql,
                "final_sql": fixed_sql
            }
        else:
            logger.warning("No rows returned from query")
            message = "No data found for your query"
            suggestions = ["Try rephrasing your question", "Check if the data exists in the system"]
            if "stock < 100" in fixed_sql:
                message = "No products have stock levels below 100; try a higher threshold"
            elif "material_name LIKE '%battery%'" in fixed_sql:
                message = "No materials with 'battery' in their name found; try filtering by tags"
            elif "DATE('2025-01-01')" in fixed_sql:
                message = "No sales data found for 2025; try a different date range"
            elif "MATCH" in fixed_sql.upper():
                message = "No matches found in full-text search"
                suggestions = ["Try broader keywords", "Check product names or tags"]
            
            result = {
                "success": False,
                "message": message,
                "suggestions": suggestions,
                "query": nl_query,
                "query_type": "simple",
                "display_format": "table",
                "summary": "No data found",
                "raw_sql": raw_sql,
                "final_sql": fixed_sql
            }
        
        _query_cache[cache_key] = result
        return result
    except Exception as e:
        logger.error(f"Execution Error: {e}")
        message = f"Execution failed on sql '{fixed_sql}': {str(e)}"
        suggestions = ["Try rephrasing your question", "Check if the data exists in the system"]
        if "no such column: fts" in str(e).lower() or "no such table: products_fts" in str(e).lower():
            message = "Full-text search table (products_fts) is missing or misconfigured"
            suggestions.append("Ensure products_fts is created and populated with data from products table")
        result = {
            "success": False,
            "message": message,
            "suggestions": suggestions,
            "query": nl_query,
            "query_type": "simple",
            "display_format": "table",
            "summary": "Execution failed",
            "raw_sql": raw_sql,
            "final_sql": fixed_sql
        }
        _query_cache[cache_key] = result
        return result