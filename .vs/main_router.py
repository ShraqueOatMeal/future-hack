import json
import re
import pandas as pd
from nlp_sql import run_nl_query
from groq_classifier import classify_intent_with_groq, run_forecast_for_topic
from productivity_analyzer import ProductivityAnalyzer
from revenue_analyzer import RevenueAnalyzer
from customer_satisfaction_analyzer import CustomerSatisfactionAnalyzer
from inventory_analyzer import InventoryAnalyzer
from stock_analyzer import StockAnalyzer
from hybrid_analyzer import HybridAnalyzer
from simple_analyzer import SimpleAnalyzer
from business_analyzer import OptimizedBusinessAnalyzer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FORBIDDEN_SQL_KEYWORDS = [
    "DELETE",
    "DROP",
    "TRUNCATE",
    "ALTER",
    "UPDATE",
    "INSERT",
    "REPLACE",
    "CREATE",
    "GRANT",
    "REVOKE",
    "EXECUTE",
    "CALL",
    "MERGE",
    "UPSERT",
    "LOAD",
    "BULK",
    "IMPORT",
    "EXPORT",
    "BACKUP",
    "RESTORE",
    "RENAME",
]

FORBIDDEN_PHRASES = [
    "delete",
    "remove",
    "drop",
    "truncate",
    "alter",
    "update",
    "insert",
    "add",
    "create",
    "modify",
    "change",
    "edit",
    "replace",
    "merge",
    "clear",
    "purge",
    "wipe",
    "erase",
    "destroy",
    "eliminate",
]


class UnifiedBusinessRouter:
    def __init__(self):
        self.analyzers = {
            "PRODUCTIVITY": ProductivityAnalyzer(),
            "REVENUE": RevenueAnalyzer(),
            "CUSTOMER_SATISFACTION": CustomerSatisfactionAnalyzer(),
            "INVENTORY": InventoryAnalyzer(),
            "STOCK": StockAnalyzer(),
            "HYBRID": HybridAnalyzer(),
            "SIMPLE": SimpleAnalyzer(),
        }
        self.business_analyzer = OptimizedBusinessAnalyzer()
        self.schema = self._get_schema()

    def _get_schema(self):
        """Retrieve the database schema for query processing."""
        return """
        Table: products
        Columns: product_id (INTEGER PRIMARY KEY), name (TEXT NOT NULL), category (TEXT NOT NULL), price (FLOAT NOT NULL), stock (INTEGER NOT NULL), supplier (TEXT NOT NULL), created_at (DATETIME), updated_at (DATETIME)

        Table: customers
        Columns: customer_id (INTEGER PRIMARY KEY), name (TEXT NOT NULL), email (TEXT NOT NULL UNIQUE), phone (TEXT), region (TEXT NOT NULL), created_at (DATETIME), updated_at (DATETIME)

        Table: sales
        Columns: sale_id (INTEGER PRIMARY KEY), date (DATE NOT NULL), product_id (INTEGER NOT NULL), customer_id (INTEGER NOT NULL), quantity (INTEGER NOT NULL), total_amount (FLOAT NOT NULL), created_at (DATETIME), updated_at (DATETIME)
        Foreign Keys: product_id REFERENCES products(product_id), customer_id REFERENCES customers(customer_id)

        Table: workers
        Columns: worker_id (INTEGER PRIMARY KEY), name (TEXT NOT NULL), department (TEXT NOT NULL), salary (FLOAT NOT NULL), performance_rating (FLOAT NOT NULL), created_at (DATETIME), updated_at (DATETIME)

        Table: inventory
        Columns: inventory_id (INTEGER PRIMARY KEY), product_id (INTEGER NOT NULL), quantity (INTEGER NOT NULL), last_updated (DATE NOT NULL), created_at (DATETIME), updated_at (DATETIME)
        Foreign Keys: product_id REFERENCES products(product_id)

        Table: materials
        Columns: material_id (INTEGER PRIMARY KEY), material_name (TEXT NOT NULL), material_type (TEXT NOT NULL), mass (FLOAT NOT NULL), created_at (DATETIME), updated_at (DATETIME)

        Table: bill_of_materials
        Columns: bom_id (INTEGER PRIMARY KEY), product_id (INTEGER NOT NULL), material_id (INTEGER), subassembly_id (INTEGER), quantity (FLOAT NOT NULL), unit (TEXT NOT NULL)
        Foreign Keys: product_id REFERENCES products(product_id), material_id REFERENCES materials(material_id), subassembly_id REFERENCES products(product_id)
        Constraints: CHECK ((material_id IS NOT NULL AND subassembly_id IS NULL) OR (material_id IS NULL AND subassembly_id IS NOT NULL))

        Table: product_tags
        Columns: product_id (INTEGER), tag (TEXT)
        Primary Key: (product_id, tag)
        Foreign Keys: product_id REFERENCES products(product_id)

        Table: material_tags
        Columns: material_id (INTEGER), tag (TEXT)
        Primary Key: (material_id, tag)
        Foreign Keys: material_id REFERENCES materials(material_id)

        Table: products_fts
        Virtual Table: USING fts5(name, content='products', content_rowid='product_id')
        """

    def process_query(self, user_query, output_format="json"):
        security_check = self._validate_security(user_query)
        if not security_check["is_safe"]:
            return self._format_response(security_check, output_format)

        intent = classify_intent_with_groq(user_query)
        logger.info(f"Classified intent: {intent}")

        if intent in self.analyzers:
            print(f"Processing {intent} query...")
            if intent == "STOCK":
                forecast_result = run_forecast_for_topic(user_query)
                result = self.analyzers[intent].analyze(user_query, forecast_result)
            elif intent == "HYBRID":
                db_result = run_nl_query(user_query)
                data = (
                    db_result.get("data", []) if db_result.get("success", False) else []
                )
                forecast_result = run_forecast_for_topic(user_query)
                result = self.analyzers[intent].analyze(
                    user_query, data, forecast_result
                )
            else:
                db_result = run_nl_query(user_query)
                data = (
                    db_result.get("data", []) if db_result.get("success", False) else []
                )
                result = self.analyzers[intent].analyze(user_query, data)

            result.update(
                {
                    "query_type": intent.lower(),
                    "query": user_query,
                    "intent": intent,
                    "database_results": db_result if intent != "STOCK" else None,
                    "forecast_results": forecast_result
                    if intent in ["STOCK", "HYBRID"]
                    else None,
                }
            )
            return self._format_response(result, output_format)

        logger.warning(
            f"Unrecognized intent: {intent}. Falling back to business intelligence."
        )
        return self._handle_business_intelligence(user_query, intent, output_format)

    def _validate_security(self, user_query):
        query_lower = user_query.lower()

        for phrase in FORBIDDEN_PHRASES:
            if phrase in query_lower:
                if any(
                    context in query_lower
                    for context in [
                        "data",
                        "record",
                        "table",
                        "database",
                        "entry",
                        "row",
                    ]
                ):
                    return {
                        "is_safe": False,
                        "error_type": "DESTRUCTIVE_OPERATION",
                        "message": f"Destructive operation '{phrase}' detected",
                        "suggestion": "Try rephrasing your query to focus on reading data instead",
                    }

        for keyword in FORBIDDEN_SQL_KEYWORDS:
            if keyword.lower() in query_lower:
                return {
                    "is_safe": False,
                    "error_type": "FORBIDDEN_SQL",
                    "message": f"SQL keyword '{keyword}' is not allowed",
                    "suggestion": "Use natural language instead of SQL commands",
                }

        suspicious_patterns = [
            (
                r"\b(delete|drop|truncate|alter|update|insert)\s+\w+",
                "SQL injection attempt",
            ),
            (
                r";\s*(delete|drop|truncate|alter|update|insert)",
                "Command chaining detected",
            ),
            (r"(--|\#|\/\*)", "SQL comment injection"),
            (r"union\s+select", "SQL union injection"),
            (r"exec\s*\(", "Code execution attempt"),
            (
                r"match\s*['\"].*?(drop|delete|truncate).*?['\"]",
                "FTS MATCH injection attempt",
            ),
        ]

        for pattern, description in suspicious_patterns:
            if re.search(pattern, query_lower, re.IGNORECASE):
                return {
                    "is_safe": False,
                    "error_type": "SUSPICIOUS_PATTERN",
                    "message": f"Security violation: {description}",
                    "suggestion": "Please use natural language for your business questions",
                }

        return {"is_safe": True}

    def _handle_business_intelligence(self, user_query, intent, output_format):
        try:
            print("Starting comprehensive business intelligence analysis...")
            db_result = run_nl_query(user_query)
            data = db_result.get("data", []) if db_result.get("success", False) else []
            insights = self.business_analyzer._get_internal_insights(
                user_query, intent, self.schema, db_data=data
            )
            result = {
                "success": True,
                "analysis": insights["metrics"],
                "trends": insights["trends"],
                "reasoning": insights["reasoning"],
                "alerts": insights["alerts"],
                "query_type": "business_intelligence",
                "query": user_query,
                "intent": intent,
                "database_results": db_result,
            }
            return self._format_response(result, output_format)
        except Exception as e:
            error_response = {
                "success": False,
                "query_type": "business_intelligence",
                "error": str(e),
                "message": "Business intelligence analysis failed",
                "suggestions": ["Try a simpler query", "Check data availability"],
            }
            return self._format_response(error_response, output_format)

    def _format_response(self, response, output_format):
        if output_format == "json":
            simplified_response = {
                "success": response.get("success", False),
                "query": response.get("query", ""),
                "intent": response.get("intent", ""),
                "query_type": response.get("query_type", ""),
                "analysis": response.get("analysis", {}),
                "reasoning": response.get("reasoning", "No reasoning provided"),
                "alerts": response.get("alerts", []),
                "database_results": response.get("database_results", None),
                "forecast_results": response.get("forecast_results", None),
            }
            return simplified_response

        if not response.get("success", False):
            return (
                f"## Error\n{response.get('error', 'Unknown error')}\n\n"
                f"**Message:** {response.get('message', 'No details available')}\n"
                f"**Suggestions:**\n"
                + "\n".join([f"- {s}" for s in response.get("suggestions", [])])
            )

        text = f"# {response.get('intent', 'Query')} Results\n\n"
        text += f"**Query:** {response.get('query', 'Unknown')}\n\n"

        # Handle analysis section
        if response.get("analysis"):
            text += "## Analysis\n\n"
            if response.get("intent") in [
                "PRODUCTIVITY",
                "REVENUE",
                "CUSTOMER_SATISFACTION",
                "INVENTORY",
                "SIMPLE",
            ]:
                # Format internal data as JSON
                if response.get("database_results") and response[
                    "database_results"
                ].get("data"):
                    text += "### Internal Data\n\n"
                    data = response["database_results"]["data"]
                    if data and isinstance(data, list) and len(data) > 0:
                        text += "```json\n"
                        text += json.dumps(data[:5], indent=2)
                        text += "\n```"
                    else:
                        text += "No internal data available.\n"

                # Include analyzer-specific analysis
                if (
                    response.get("intent") == "REVENUE"
                    and "by_region" in response["analysis"]
                ):
                    text += "\n### Revenue by Region\n\n"
                    regions = response["analysis"].get("by_region", [])
                    if regions:
                        text += "```json\n"
                        text += json.dumps(regions, indent=2)
                        text += "\n```"
                    text += f"\n- **Total Revenue**: {response['analysis'].get('total_revenue', 0):,.2f}\n"
                    text += f"- **Average Revenue per Region**: {response['analysis'].get('average_revenue_per_region', 0):,.2f}\n"
                    text += f"- **Region Count**: {response['analysis'].get('region_count', 0)}\n"
                else:
                    for key, value in response["analysis"].items():
                        if isinstance(value, (int, float)):
                            text += (
                                f"- **{key.replace('_', ' ').title()}**: {value:,.2f}\n"
                            )
                        elif (
                            isinstance(value, list)
                            and value
                            and isinstance(value[0], dict)
                        ):
                            text += f"\n### {key.replace('_', ' ').title()}\n\n"
                            text += "```json\n"
                            text += json.dumps(value[:5], indent=2)
                            text += "\n```"
                        else:
                            text += f"- **{key.replace('_', ' ').title()}**: {value}\n"
            elif response.get("intent") in ["STOCK", "HYBRID"]:
                if response.get("forecast_results"):
                    text += "### Stock Predictions\n\n"
                    forecast = response["forecast_results"]
                    if isinstance(forecast, dict) and "results" in forecast:
                        for pred in forecast["results"]:
                            symbol = pred.get("symbol", "Unknown")
                            price = pred.get("prediction", "N/A")
                            trend = pred.get("trend", "N/A")
                            text += f"- **{symbol}**: Predicted closing price: ${price} (Trend: {trend})\n"
                    else:
                        text += "No stock predictions available.\n"
                if response.get("intent") == "HYBRID" and response.get(
                    "analysis", {}
                ).get("internal_metrics"):
                    text += "\n### Internal Metrics\n\n"
                    for key, value in response["analysis"]["internal_metrics"].items():
                        if isinstance(value, (int, float)):
                            text += (
                                f"- **{key.replace('_', ' ').title()}**: {value:,.2f}\n"
                            )
                        else:
                            text += f"- **{key.replace('_', ' ').title()}**: {value}\n"
            else:
                for key, value in response["analysis"].items():
                    if isinstance(value, list) and value and isinstance(value[0], dict):
                        text += f"### {key.replace('_', ' ').title()}\n\n"
                        text += "```json\n"
                        text += json.dumps(value[:5], indent=2)
                        text += "\n```"
                    else:
                        text += f"- **{key.replace('_', ' ').title()}**: {value}\n"

        # Trends section
        if response.get("trends"):
            text += "\n## Trends\n\n"
            for key, value in response["trends"].items():
                text += f"- **{key.replace('_', ' ').title()}**: {value}\n"

        # Reasoning section
        if response.get("reasoning"):
            text += "\n## Reasoning\n\n"
            if isinstance(response["reasoning"], dict):
                for key, value in response["reasoning"].items():
                    text += f"- **{key.replace('_', ' ').title()}**: {value}\n"
            else:
                text += response["reasoning"]

        # Alerts section
        if response.get("alerts"):
            text += "\n## Alerts\n\n"
            for alert in response["alerts"]:
                text += f"- {alert}\n"

        return text


def route_query(user_query):
    router = UnifiedBusinessRouter()
    return router.process_query(user_query, output_format="text")


def route_query_json(user_query):
    router = UnifiedBusinessRouter()
    return router.process_query(user_query, output_format="json")


if __name__ == "__main__":
    router = UnifiedBusinessRouter()
    while True:
        query = input("\nAsk a question (or 'exit'): ").strip()
        if query.lower() in ("exit", "quit", "q"):
            print("\nExit System!")
            break
        if not query:
            continue

        print(f"\nProcessing: '{query}'")
        result = router.process_query(query, output_format="text")
        print(result)
        print("=" * 80)
