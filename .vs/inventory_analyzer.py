from nlp_sql import run_nl_query

class InventoryAnalyzer:
    def analyze(self, query, data):
        """
        Analyze inventory metrics from query results.
        
        Args:
            query (str): The user's natural language query.
            data (list): Data returned from run_nl_query.
        
        Returns:
            dict: Analysis results and reasoning.
        """
        # Validate input data
        if not data or not isinstance(data, list):
            # Fallback query for inventory data
            fallback_query = "Show products with stock below 100"
            db_result = run_nl_query(fallback_query)
            data = db_result.get("data", []) if db_result.get("success", False) else []
            reasoning = "No data provided; used fallback query to retrieve inventory data."
        else:
            reasoning = f"Analyzed {len(data)} inventory records from the original query: '{query}'."

        # Process inventory data
        try:
            valid_records = [
                row for row in data 
                if 'quantity' in row 
                and isinstance(row['quantity'], (int, float)) 
                and 'product_name' in row
            ]
            if not valid_records:
                return {
                    "success": False,
                    "error": "No valid inventory data found",
                    "reasoning": f"{reasoning} No records contained valid 'quantity' and 'product_name' fields."
                }
            
            low_stock = [row for row in valid_records if row['quantity'] < 100]
            total_products = len(valid_records)
            low_stock_count = len(low_stock)
            
            reasoning += (
                f" Identified {low_stock_count} products with stock below 100 out of {total_products} "
                f"total products analyzed."
            )
            
            return {
                "success": True,
                "analysis": {
                    "low_stock_count": low_stock_count,
                    "total_products": total_products,
                    "low_stock_products": [
                        {"name": row['product_name'], "quantity": row['quantity']} 
                        for row in low_stock
                    ]
                },
                "reasoning": reasoning
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"Analysis failed: {str(e)}",
                "reasoning": f"{reasoning} Error processing inventory data: {str(e)}."
            }