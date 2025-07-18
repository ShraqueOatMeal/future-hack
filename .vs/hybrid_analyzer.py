from nlp_sql import run_nl_query
from forecast import run_forecast_for_topic

class HybridAnalyzer:
    def analyze(self, query, data=None, forecast_data=None):
        """
        Analyze combined internal and external data.
        
        Args:
            query (str): The user's natural language query.
            data (list): Internal database data from run_nl_query.
            forecast_data (dict): External forecast data from run_forecast_for_topic.
        
        Returns:
            dict: Analysis results and reasoning.
        """
        # Fetch internal data
        if not data or not isinstance(data, list):
            fallback_query = "Show sales, inventory, or productivity data relevant to the query"
            db_result = run_nl_query(fallback_query)
            data = db_result.get("data", []) if db_result.get("success", False) else []
            reasoning = "No internal data provided; used fallback query for database data."
        else:
            reasoning = f"Analyzed {len(data)} internal records from query: '{query}'."

        # Fetch external forecast
        forecast_result = forecast_data if forecast_data else run_forecast_for_topic(query)
        reasoning += " Using pre-fetched forecast data." if forecast_data else f" Generated forecast for query: '{query}'."

        # Process combined data
        try:
            internal_metrics = {}
            if data:
                # Basic processing of internal data (e.g., revenue, inventory, productivity)
                if any('revenue' in row for row in data):
                    total_revenue = sum(row['revenue'] for row in data if 'revenue' in row and isinstance(row['revenue'], (int, float)))
                    internal_metrics["total_revenue"] = total_revenue
                elif any('quantity' in row for row in data):
                    low_stock_count = len([row for row in data if 'quantity' in row and isinstance(row['quantity'], (int, float)) and row['quantity'] < 100])
                    internal_metrics["low_stock_count"] = low_stock_count
                elif any('output' in row and 'hours_worked' in row for row in data):
                    productivity = [row['output'] / row['hours_worked'] for row in data if 'output' in row and 'hours_worked' in row and row['hours_worked'] > 0]
                    internal_metrics["average_productivity"] = sum(productivity) / len(productivity) if productivity else 0
            
            if not forecast_result.get("success", False):
                return {
                    "success": False,
                    "error": forecast_result.get("message", "No stock data available"),
                    "reasoning": f"{reasoning} Forecast failed: {forecast_result.get('message', 'Unknown error')}."
                }
            
            symbols = forecast_result.get("symbols", [])
            external_metrics = {
                "symbols_analyzed": symbols,
                "predictions": [
                    {"symbol": r["symbol"], "prediction": r["prediction"], "trend": r["trend"]}
                    for r in forecast_result.get("results", [])
                ]
            }
            
            reasoning += (
                f" Combined {len(data)} internal records with stock forecasts for {len(symbols)} symbols: {', '.join(symbols)}."
            )
            
            return {
                "success": True,
                "analysis": {
                    "internal_metrics": internal_metrics,
                    "external_metrics": external_metrics
                },
                "reasoning": reasoning
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"Analysis failed: {str(e)}",
                "reasoning": f"{reasoning} Error processing hybrid data: {str(e)}."
            }