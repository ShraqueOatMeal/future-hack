from nlp_sql import run_nl_query

class SimpleAnalyzer:
    def analyze(self, query, data):
        """
        Handle simple data retrieval queries.
        
        Args:
            query (str): The user's natural language query.
            data (list): Data returned from run_nl_query.
        
        Returns:
            dict: Analysis results and reasoning.
        """
        # Validate input data
        if not data or not isinstance(data, list):
            # Use the original query as it’s likely specific
            db_result = run_nl_query(query)
            data = db_result.get("data", []) if db_result.get("success", False) else []
            reasoning = f"Queried database with original query: '{query}'."
        else:
            reasoning = f"Analyzed {len(data)} records from provided data for query: '{query}'."

        # Process simple query results
        try:
            if not data:
                return {
                    "success": False,
                    "error": "No data found",
                    "reasoning": f"{reasoning} Query returned no results."
                }
            
            columns = list(data[0].keys()) if data else []
            record_count = len(data)
            
            reasoning += f" Retrieved {record_count} records with columns: {', '.join(columns)}."
            
            return {
                "success": True,
                "analysis": {
                    "data": data,
                    "record_count": record_count,
                    "columns": columns
                },
                "reasoning": reasoning
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"Analysis failed: {str(e)}",
                "reasoning": f"{reasoning} Error processing simple query data: {str(e)}."
            }