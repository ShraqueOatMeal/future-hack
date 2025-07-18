from nlp_sql import run_nl_query

class CustomerSatisfactionAnalyzer:
    def analyze(self, query, data):
        """
        Analyze customer satisfaction metrics from query results.
        
        Args:
            query (str): The user's natural language query.
            data (list): Data returned from run_nl_query.
        
        Returns:
            dict: Analysis results and reasoning.
        """
        # Validate input data
        if not data or not isinstance(data, list):
            # Fallback query for customer satisfaction data
            fallback_query = "Show customer ratings and feedback for the last quarter"
            db_result = run_nl_query(fallback_query)
            data = db_result.get("data", []) if db_result.get("success", False) else []
            reasoning = "No data provided; used fallback query to retrieve customer satisfaction data."
        else:
            reasoning = f"Analyzed {len(data)} customer satisfaction records from the original query: '{query}'."

        # Process customer satisfaction data
        try:
            valid_records = [
                row for row in data 
                if 'rating' in row 
                and isinstance(row['rating'], (int, float)) 
                and 0 <= row['rating'] <= 5
            ]
            if not valid_records:
                return {
                    "success": False,
                    "error": "No valid customer satisfaction data found",
                    "reasoning": f"{reasoning} No records contained valid 'rating' field (0-5 scale)."
                }
            
            avg_rating = sum(row['rating'] for row in valid_records) / len(valid_records) if valid_records else 0
            trend = "improving" if len(valid_records) > 1 and valid_records[-1]['rating'] > valid_records[0]['rating'] else "stable"
            
            reasoning += (
                f" Calculated average customer rating as {avg_rating:.2f}/5 from {len(valid_records)} "
                f"valid records with ratings. Trend: {trend}."
            )
            
            return {
                "success": True,
                "analysis": {
                    "average_rating": avg_rating,
                    "record_count": len(valid_records),
                    "trend": trend
                },
                "reasoning": reasoning
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"Analysis failed: {str(e)}",
                "reasoning": f"{reasoning} Error processing customer satisfaction data: {str(e)}."
            }