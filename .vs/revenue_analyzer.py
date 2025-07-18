from nlp_sql import run_nl_query
import logging

logger = logging.getLogger(__name__)

class RevenueAnalyzer:
    def analyze(self, user_query, db_data):
        """Analyze revenue data based on user query and database results."""
        try:
            # Use pre-fetched data if available, otherwise query the database
            if db_data and self._validate_data(db_data):
                data = db_data
                reasoning = "Using pre-fetched database results for revenue analysis"
            else:
                query = self._formulate_query(user_query)
                result = run_nl_query(query)
                data = result.get("data", []) if result.get("success", False) else []
                reasoning = f"Queried database with: '{query}'"
            
            if not data:
                return {
                    "success": False,
                    "error": "No valid revenue data found",
                    "reasoning": reasoning + ". No data returned from query."
                }
            
            # Process data to extract metrics and alerts
            metrics, alerts = self._process_revenue_data(data, user_query)
            trends = self._calculate_trends(data)
            
            return {
                "success": True,
                "analysis": metrics,
                "reasoning": reasoning + f". Processed {len(data)} records to compute revenue metrics.",
                "trends": trends,
                "alerts": alerts
            }
        except Exception as e:
            logger.error(f"Revenue analysis failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "reasoning": "Revenue analysis failed due to an unexpected error."
            }
    
    def _formulate_query(self, user_query):
        """Formulate a fallback query if no pre-fetched data is provided."""
        if "region" in user_query.lower():
            return (
                f"SELECT c.region, DATE(s.date) AS sale_date, SUM(s.total_amount) AS total_revenue "
                f"FROM sales s JOIN customers c ON s.customer_id = c.customer_id "
                f"WHERE s.date LIKE '2025%' GROUP BY c.region, DATE(s.date) ORDER BY c.region, sale_date"
            )
        return (
            f"SELECT DATE(date) AS sale_date, SUM(total_amount) AS total_revenue "
            f"FROM sales WHERE date LIKE '2025%' GROUP BY DATE(date) ORDER BY sale_date"
        )
    
    def _validate_data(self, data):
        """Validate that the data contains required revenue fields."""
        if not data or not isinstance(data, list):
            logger.debug("Validation failed: Data is empty or not a list")
            return False
        required_fields = ["total_revenue", "total_amount", "revenue"]
        first_row = data[0]
        logger.debug(f"Validating data with keys: {list(first_row.keys())}")
        return any(field in first_row for field in required_fields)
    
    def _process_revenue_data(self, data, user_query):
        """Process revenue data to extract metrics and alerts."""
        metrics = {}
        alerts = []
        
        if "region" in user_query.lower():
            # Aggregate by region
            regions = {}
            for row in data:
                region = row.get("region", "Unknown")
                revenue = float(row.get("total_revenue", 0))
                if region not in regions:
                    regions[region] = {"total_revenue": 0, "days": []}
                regions[region]["total_revenue"] += revenue
                regions[region]["days"].append({"sale_date": row.get("sale_date"), "revenue": revenue})
            
            # Generate metrics for each region
            metrics["by_region"] = [
                {
                    "region": region,
                    "total_revenue": info["total_revenue"],
                    "record_count": len(info["days"])
                }
                for region, info in regions.items()
            ]
            metrics["total_revenue"] = sum(info["total_revenue"] for info in regions.values())
            metrics["region_count"] = len(regions)
            metrics["average_revenue_per_region"] = (
                metrics["total_revenue"] / metrics["region_count"] if metrics["region_count"] > 0 else 0
            )
            
            # Generate alerts for low-revenue regions
            for region, info in regions.items():
                if info["total_revenue"] < 1000000:  # Example threshold
                    alerts.append(f"Low revenue in {region}: {info['total_revenue']:,.2f}")
        
        else:
            # Aggregate by date
            total_revenue = 0
            daily_revenues = []
            for row in data:
                revenue = float(row.get("total_revenue", 0))
                total_revenue += revenue
                daily_revenues.append({"sale_date": row.get("sale_date"), "revenue": revenue})
            
            metrics["total_revenue"] = total_revenue
            metrics["record_count"] = len(data)
            metrics["average_daily_revenue"] = total_revenue / len(data) if data else 0
            metrics["daily_revenues"] = daily_revenues
            
            # Generate alerts for low-revenue days
            for day in daily_revenues:
                if day["revenue"] < 100000:  # Example threshold
                    alerts.append(f"Low revenue on {day['sale_date']}: {day['revenue']:,.2f}")
        
        return metrics, alerts
    
    def _calculate_trends(self, data):
        """Calculate revenue trends."""
        trends = {}
        if not data:
            return trends
        
        if "region" in data[0]:
            regions = set(row["region"] for row in data)
            for region in regions:
                region_data = [row for row in data if row["region"] == region]
                revenues = [float(row["total_revenue"]) for row in region_data]
                if len(revenues) > 1:
                    trend = (
                        "increasing" if revenues[-1] > revenues[0]
                        else "decreasing" if revenues[-1] < revenues[0]
                        else "stable"
                    )
                    trends[f"revenue_{region.lower()}"] = trend
        else:
            revenues = [float(row.get("total_revenue", 0)) for row in data]
            if len(revenues) > 1:
                trends["revenue"] = (
                    "increasing" if revenues[-1] > revenues[0]
                    else "decreasing" if revenues[-1] < revenues[0]
                    else "stable"
                )
        
        return trends