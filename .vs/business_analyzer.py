import re
from nlp_sql import run_nl_query
from productivity_analyzer import ProductivityAnalyzer
from revenue_analyzer import RevenueAnalyzer
from customer_satisfaction_analyzer import CustomerSatisfactionAnalyzer
from inventory_analyzer import InventoryAnalyzer


class OptimizedBusinessAnalyzer:
    def __init__(self):
        # Initialize analyzers for specific intents
        self.analyzers = {
            "PRODUCTIVITY": ProductivityAnalyzer(),
            "REVENUE": RevenueAnalyzer(),
            "CUSTOMER_SATISFACTION": CustomerSatisfactionAnalyzer(),
            "INVENTORY": InventoryAnalyzer(),
        }

    def _get_internal_insights(self, user_query, intent, schema, db_data=None):
        """Get key internal metrics with reasoning, based on dynamic schema and pre-fetched data.

        Args:
            user_query (str): The user's natural language query.
            intent (str): The classified intent (e.g., PRODUCTIVITY, REVENUE).
            schema (str): Database schema information.
            db_data (list, optional): Pre-fetched database results.

        Returns:
            dict: Insights including metrics, alerts, trends, and reasoning.
        """
        insights = {"metrics": {}, "alerts": [], "trends": {}, "reasoning": {}}

        # Use pre-fetched data if available
        if db_data:
            insights["metrics"]["pre_fetched_data"] = db_data
            insights["reasoning"]["pre_fetched_data"] = (
                "Using pre-fetched database results"
            )

        # Identify relevant tables from schema
        tables = re.findall(r"Table: (\w+)", schema)
        inventory_tables = [
            t
            for t in tables
            if any(k in t.lower() for k in ["inventory", "stock", "supply"])
        ]
        sales_tables = [
            t
            for t in tables
            if any(k in t.lower() for k in ["sales", "order", "transaction"])
        ]
        product_tables = [
            t
            for t in tables
            if any(k in t.lower() for k in ["product", "item", "component"])
        ]
        material_tables = [t for t in tables if "material" in t.lower()]
        employee_tables = [
            t
            for t in tables
            if any(k in t.lower() for k in ["employee", "staff", "worker"])
        ]
        customer_tables = [
            t
            for t in tables
            if any(k in t.lower() for k in ["customer", "client", "feedback", "rating"])
        ]

        # Delegate to specific analyzer based on intent
        if intent in self.analyzers and not db_data:
            # If no pre-fetched data, let the analyzer fetch and process data
            result = self.analyzers[intent].analyze(user_query, [])
            if result.get("success", False):
                insights["metrics"][intent.lower()] = result.get("analysis", {})
                insights["reasoning"][intent.lower()] = result.get(
                    "reasoning", "Analysis performed by dedicated module"
                )
                if "trend" in result.get("analysis", {}):
                    insights["trends"][intent.lower()] = result["analysis"]["trend"]
            else:
                insights["alerts"].append(result.get("error", "Analysis failed"))
                insights["reasoning"][intent.lower()] = result.get(
                    "reasoning", "No reasoning provided"
                )
        elif intent in self.analyzers and db_data:
            # Use pre-fetched data with the appropriate analyzer
            result = self.analyzers[intent].analyze(user_query, db_data)
            if result.get("success", False):
                insights["metrics"][intent.lower()] = result.get("analysis", {})
                insights["reasoning"][intent.lower()] = result.get(
                    "reasoning", "Analysis performed on pre-fetched data"
                )
                if "trend" in result.get("analysis", {}):
                    insights["trends"][intent.lower()] = result["analysis"]["trend"]
            else:
                insights["alerts"].append(result.get("error", "Analysis failed"))
                insights["reasoning"][intent.lower()] = result.get(
                    "reasoning", "No reasoning provided"
                )

        # Core business metrics for non-specific intents or fallback
        queries = {}
        if intent not in self.analyzers or intent == "SIMPLE":
            if sales_tables and not db_data:
                queries["revenue_trend"] = (
                    f"Show daily revenue for the last 30 days from {sales_tables[0]}"
                )
                queries["top_products"] = (
                    f"Show top 5 products by revenue from {sales_tables[0]}"
                )
            if inventory_tables and not db_data:
                queries["inventory_alerts"] = (
                    f"Show products with stock below 100 from {inventory_tables[0]}"
                )
            if product_tables and sales_tables and not db_data:
                queries["customer_segments"] = (
                    f"Show revenue by customer region from {sales_tables[0]}"
                )
                queries["sales_performance"] = (
                    f"Show total sales and transaction counts by product category from {sales_tables[0]}"
                )

        # Add production capacity and material bottleneck queries for manufacturing-related queries
        if (
            any(
                keyword in user_query.lower()
                for keyword in [
                    "manufacture",
                    "produce",
                    "production",
                    "units",
                    "inventory",
                    "component",
                    "material",
                ]
            )
            and not db_data
        ):
            if inventory_tables and product_tables:
                queries["production_capacity"] = (
                    f"SELECT p.name, i.quantity, p.tags "
                    f"FROM {inventory_tables[0]} i "
                    f"JOIN {product_tables[0]} p ON i.product_id = p.product_id "
                    f"WHERE LOWER(p.tags) LIKE '%component%'"
                )
                queries["bottleneck_analysis"] = (
                    f"SELECT p.name, i.quantity, p.tags "
                    f"FROM {inventory_tables[0]} i "
                    f"JOIN {product_tables[0]} p ON i.product_id = p.product_id "
                    f"WHERE LOWER(p.tags) LIKE '%component%' "
                    f"ORDER BY i.quantity ASC LIMIT 1"
                )
            if material_tables:
                queries["material_bottleneck"] = (
                    f"SELECT m.material_name, m.material_type, m.tags, b.quantity "
                    f"FROM {material_tables[0]} m "
                    f"JOIN bill_of_materials b ON m.material_id = b.material_id "
                    f"JOIN products p ON b.product_id = p.product_id "
                    f"JOIN {inventory_tables[0]} i ON p.product_id = i.product_id "
                    f"WHERE LOWER(m.tags) LIKE '%material%' "
                    f"ORDER BY b.quantity ASC LIMIT 1"
                )
                if "battery" in user_query.lower():
                    queries["battery_materials"] = (
                        f"SELECT m.material_name, m.material_type, m.tags, i.quantity "
                        f"FROM {material_tables[0]} m "
                        f"JOIN {inventory_tables[0]} i ON m.product_id = i.product_id "
                        f"WHERE LOWER(m.tags) LIKE '%battery%'"
                    )

        # Execute fallback queries if no pre-fetched data
        for metric_name, nl_query in queries.items():
            try:
                data = run_nl_query(nl_query)
                if data and not isinstance(data, dict) or "error" not in data:
                    insights["metrics"][metric_name] = data
                    insights["reasoning"][metric_name] = self._explain_metric(
                        metric_name, data, user_query
                    )
                else:
                    insights["alerts"].append(
                        f"Could not fetch {metric_name}: {data.get('message', 'Unknown error')}"
                    )
            except Exception as e:
                insights["alerts"].append(f"Could not fetch {metric_name}: {str(e)}")

        # Calculate production capacity
        if (
            "production_capacity" in insights["metrics"]
            or "material_bottleneck" in insights["metrics"]
        ):
            insights["production_capacity"] = self._calculate_production_capacity(
                insights["metrics"].get("production_capacity", []),
                insights["metrics"].get("material_bottleneck", []),
                user_query,
            )

        # Calculate trends for non-analyzer metrics
        insights["trends"].update(self._calculate_trends(insights["metrics"]))

        return insights

    def _explain_metric(self, metric_name, data, user_query):
        """Generate reasoning for a specific metric."""
        reasoning = f"Retrieved {metric_name} for query: '{user_query}'."
        if isinstance(data, list) and data:
            columns = list(data[0].keys()) if data else []
            reasoning += f" Data contains {len(data)} records with columns: {', '.join(columns)}."
        elif isinstance(data, dict) and "error" not in data:
            reasoning += f" Data contains aggregated metrics: {list(data.keys())}."
        else:
            reasoning += " No valid data returned."
        return reasoning

    def _calculate_production_capacity(
        self, capacity_data, bottleneck_data, user_query
    ):
        """Calculate production capacity based on component and material data."""
        try:
            capacity = {"available_units": 0, "limiting_factor": "unknown"}
            if capacity_data and isinstance(capacity_data, list):
                total_units = sum(
                    row.get("quantity", 0)
                    for row in capacity_data
                    if isinstance(row.get("quantity"), (int, float))
                )
                capacity["available_units"] = total_units
                capacity["limiting_factor"] = "component inventory"
            if (
                bottleneck_data
                and isinstance(bottleneck_data, list)
                and bottleneck_data
            ):
                limiting_item = bottleneck_data[0].get("name", "unknown")
                limiting_quantity = bottleneck_data[0].get("quantity", 0)
                capacity["limiting_factor"] = (
                    f"{limiting_item} (quantity: {limiting_quantity})"
                )
            return capacity
        except Exception as e:
            return {"error": f"Production capacity calculation failed: {str(e)}"}

    def _calculate_trends(self, metrics):
        """Calculate trends for metrics."""
        trends = {}
        for metric_name, data in metrics.items():
            if isinstance(data, list) and len(data) > 1:
                key_field = (
                    "revenue"
                    if "revenue" in data[0]
                    else "quantity"
                    if "quantity" in data[0]
                    else None
                )
                if key_field:
                    values = [
                        row[key_field]
                        for row in data
                        if isinstance(row[key_field], (int, float))
                    ]
                    trend = (
                        "increasing"
                        if values[-1] > values[0]
                        else "decreasing"
                        if values[-1] < values[0]
                        else "stable"
                    )
                    trends[metric_name] = trend
        return trends

