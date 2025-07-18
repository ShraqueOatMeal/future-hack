from nlp_sql import run_nl_query
import logging

logger = logging.getLogger(__name__)

class ProductivityAnalyzer:
    def analyze(self, user_query, db_data):
        """Analyze productivity data based on user query and database results."""
        try:
            # Use pre-fetched data if available, otherwise query the database
            if db_data and self._validate_data(db_data):
                data = db_data
                reasoning = "Using pre-fetched database results for productivity analysis"
            else:
                query = self._formulate_query(user_query)
                result = run_nl_query(query)
                data = result.get("data", []) if result.get("success", False) else []
                reasoning = f"Queried database with: '{query}'"
            
            if not data:
                return {
                    "success": False,
                    "error": "No valid productivity data found",
                    "reasoning": reasoning + ". No data returned from query."
                }
            
            # Process data to extract metrics and alerts
            metrics, alerts = self._process_productivity_data(data, user_query)
            trends = self._calculate_trends(data)
            
            return {
                "success": True,
                "analysis": metrics,
                "reasoning": reasoning + f". Processed {len(data)} records to compute productivity metrics.",
                "trends": trends,
                "alerts": alerts
            }
        except Exception as e:
            logger.error(f"Productivity analysis failed: {str(e)}")
            return {
                "success": False,
                "error": str(e),
                "reasoning": "Productivity analysis failed due to an unexpected error."
            }
    
    def _formulate_query(self, user_query):
        """Formulate a query for worker performance data."""
        if "department" in user_query.lower():
            return (
                f"SELECT department, name, performance_rating, updated_at "
                f"FROM workers WHERE department = 'manufacturing' AND updated_at LIKE '2025%'"
            )
        return (
            f"SELECT department, name, performance_rating, updated_at "
            f"FROM workers WHERE updated_at LIKE '2025%'"
        )
    
    def _validate_data(self, data):
        """Validate that the data contains required productivity fields."""
        if not data or not isinstance(data, list):
            logger.debug("Validation failed: Data is empty or not a list")
            return False
        required_fields = ["performance_rating"]
        first_row = data[0]
        logger.debug(f"Validating data with keys: {list(first_row.keys())}")
        return any(field in first_row for field in required_fields)
    
    def _process_productivity_data(self, data, user_query):
        """Process productivity data to extract metrics and alerts."""
        metrics = {}
        alerts = []
        
        if "department" in user_query.lower():
            # Aggregate by department (focused on manufacturing)
            departments = {}
            for row in data:
                department = row.get("department", "Unknown")
                rating = float(row.get("performance_rating", 0))
                if department not in departments:
                    departments[department] = {
                        "total_rating": 0,
                        "worker_count": 0,
                        "workers": []
                    }
                departments[department]["total_rating"] += rating
                departments[department]["worker_count"] += 1
                departments[department]["workers"].append({
                    "name": row.get("name"),
                    "performance_rating": rating,
                    "updated_at": row.get("updated_at")
                })
            
            # Generate metrics for each department
            metrics["by_department"] = [
                {
                    "department": dept,
                    "average_rating": info["total_rating"] / info["worker_count"] if info["worker_count"] > 0 else 0,
                    "worker_count": info["worker_count"],
                    "top_performers": sum(1 for w in info["workers"] if w["performance_rating"] >= 4.5)
                }
                for dept, info in departments.items()
            ]
            metrics["total_workers"] = sum(info["worker_count"] for info in departments.values())
            metrics["average_rating"] = (
                sum(info["total_rating"] for info in departments.values()) / metrics["total_workers"]
                if metrics["total_workers"] > 0 else 0
            )
            
            # Generate alerts for low-performing departments
            for dept, info in departments.items():
                avg_rating = info["total_rating"] / info["worker_count"] if info["worker_count"] > 0 else 0
                if avg_rating < 3.0:  # Example threshold
                    alerts.append(f"Low average performance in {dept}: {avg_rating:.2f}")
        
        else:
            # Aggregate across all workers
            total_rating = 0
            workers = []
            for row in data:
                rating = float(row.get("performance_rating", 0))
                total_rating += rating
                workers.append({
                    "name": row.get("name"),
                    "performance_rating": rating,
                    "updated_at": row.get("updated_at")
                })
            
            metrics["total_workers"] = len(data)
            metrics["average_rating"] = total_rating / len(data) if data else 0
            metrics["top_performers"] = sum(1 for w in workers if w["performance_rating"] >= 4.5)
            metrics["workers"] = workers
            
            # Generate alerts for low-performing workers
            for worker in workers:
                if worker["performance_rating"] < 3.0:  # Example threshold
                    alerts.append(f"Low performance for {worker['name']}: {worker['performance_rating']:.2f}")
        
        return metrics, alerts
    
    def _calculate_trends(self, data):
        """Calculate performance trends."""
        trends = {}
        if not data:
            return trends
        
        if "department" in data[0]:
            departments = set(row["department"] for row in data)
            for dept in departments:
                dept_data = [row for row in data if row["department"] == dept]
                ratings = [float(row["performance_rating"]) for row in dept_data]
                if len(ratings) > 1:
                    trend = (
                        "improving" if ratings[-1] > ratings[0]
                        else "declining" if ratings[-1] < ratings[0]
                        else "stable"
                    )
                    trends[f"performance_{dept.lower()}"] = trend
        else:
            ratings = [float(row.get("performance_rating", 0)) for row in data]
            if len(ratings) > 1:
                trends["performance"] = (
                    "improving" if ratings[-1] > ratings[0]
                    else "declining" if ratings[-1] < ratings[0]
                    else "stable"
                )
        
        return trends