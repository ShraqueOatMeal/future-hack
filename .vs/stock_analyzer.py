from forecast import run_forecast_for_topic

class StockAnalyzer:
    def analyze(self, query, data=None):
        """
        Analyze stock market data from forecast results.
        
        Args:
            query (str): The user's natural language query.
            data (dict): Pre-fetched forecast data (optional).
        
        Returns:
            dict: Analysis results and reasoning.
        """
        # Fetch stock forecast
        forecast_result = data if data else run_forecast_for_topic(query)
        reasoning = "Using pre-fetched forecast data." if data else f"Generated forecast for query: '{query}'."

        # Process forecast data
        try:
            if not forecast_result.get("success", False):
                return {
                    "success": False,
                    "error": forecast_result.get("message", "No stock data available"),
                    "reasoning": f"{reasoning} Forecast failed: {forecast_result.get('message', 'Unknown error')}."
                }
            
            symbols = forecast_result.get("symbols", [])
            results = forecast_result.get("results", [])
            summary = forecast_result.get("summary", "No summary available")
            
            analysis = {
                "symbols_analyzed": symbols,
                "predictions": [
                    {"symbol": r["symbol"], "prediction": r["prediction"], "trend": r["trend"]}
                    for r in results
                ],
                "summary": summary
            }
            
            reasoning += (
                f" Analyzed stock data for {len(symbols)} symbols: {', '.join(symbols)}. "
                f"Generated predictions based on recent market trends."
            )
            
            return {
                "success": True,
                "analysis": analysis,
                "reasoning": reasoning
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"Analysis failed: {str(e)}",
                "reasoning": f"{reasoning} Error processing stock forecast data: {str(e)}."
            }