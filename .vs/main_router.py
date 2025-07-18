import json
import re
import pandas as pd
from nlp_sql import run_nl_query
from business_analyzer import OptimizedBusinessAnalyzer
from groq_classifier import run_forecast_for_topic
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FORBIDDEN_SQL_KEYWORDS = [
    'DELETE', 'DROP', 'TRUNCATE', 'ALTER', 'UPDATE', 'INSERT', 'REPLACE',
    'CREATE', 'GRANT', 'REVOKE', 'EXECUTE', 'CALL', 'MERGE', 'UPSERT',
    'LOAD', 'BULK', 'IMPORT', 'EXPORT', 'BACKUP', 'RESTORE', 'RENAME'
]

FORBIDDEN_PHRASES = [
    'delete', 'remove', 'drop', 'truncate', 'alter', 'update', 'insert',
    'add', 'create', 'modify', 'change', 'edit', 'replace', 'merge',
    'clear', 'purge', 'wipe', 'erase', 'destroy', 'eliminate'
]

class UnifiedBusinessRouter:
    def __init__(self):
        self.bi_analyzer = OptimizedBusinessAnalyzer()
    
    def process_query(self, user_query, output_format="json"):
        security_check = self._validate_security(user_query)
        if not security_check["is_safe"]:
            return self._format_response(security_check, output_format)
        
        if self._is_hybrid_query(user_query):
            return self._handle_hybrid_query(user_query, output_format)
        elif self._is_simple_query(user_query):
            return self._handle_simple_query(user_query, output_format)
        else:
            return self._handle_business_intelligence(user_query, output_format)
    
    def _validate_security(self, user_query):
        query_lower = user_query.lower()
        
        for phrase in FORBIDDEN_PHRASES:
            if phrase in query_lower:
                if any(context in query_lower for context in ['data', 'record', 'table', 'database', 'entry', 'row']):
                    return {
                        "is_safe": False,
                        "error_type": "DESTRUCTIVE_OPERATION",
                        "message": f"Destructive operation '{phrase}' detected",
                        "suggestion": "Try rephrasing your query to focus on reading data instead"
                    }
        
        for keyword in FORBIDDEN_SQL_KEYWORDS:
            if keyword.lower() in query_lower:
                return {
                    "is_safe": False,
                    "error_type": "FORBIDDEN_SQL",
                    "message": f"SQL keyword '{keyword}' is not allowed",
                    "suggestion": "Use natural language instead of SQL commands"
                }
        
        suspicious_patterns = [
            (r'\b(delete|drop|truncate|alter|update|insert)\s+\w+', "SQL injection attempt"),
            (r';\s*(delete|drop|truncate|alter|update|insert)', "Command chaining detected"),
            (r'(--|\#|\/\*)', "SQL comment injection"),
            (r'union\s+select', "SQL union injection"),
            (r'exec\s*\(', "Code execution attempt"),
            (r"match\s*['\"].*?(drop|delete|truncate).*?['\"]", "FTS MATCH injection attempt")
        ]
        
        for pattern, description in suspicious_patterns:
            if re.search(pattern, query_lower, re.IGNORECASE):
                return {
                    "is_safe": False,
                    "error_type": "SUSPICIOUS_PATTERN",
                    "message": f"Security violation: {description}",
                    "suggestion": "Please use natural language for your business questions"
                }
        
        return {"is_safe": True}
    
    def _is_simple_query(self, user_query):
        simple_patterns = [
            r'^(show|list|get|display|find|search)\s+\w+',
            r'^what\s+(is|are)\s+\w+',
            r'^how\s+many\s+\w+',
            r'just\s+(show|list|get)',
            r'(simple|quick|only)\s+(show|list|get)',
            r'^(top|bottom)\s+\d+',
            r'^(count|sum|total|average)\s+',
            r'data\s+for\s+',
            r'information\s+about\s+'
        ]
        
        query_lower = user_query.lower()
        for pattern in simple_patterns:
            if re.search(pattern, query_lower):
                return True
        
        bi_keywords = [
            'recommend', 'suggest', 'should', 'strategy', 'improve', 'optimize',
            'invest', 'opportunity', 'growth', 'profit', 'market', 'competitive',
            'forecast', 'predict', 'trend', 'analysis', 'insight', 'advice',
            'opportunity', 'risk', 'performance', 'compare', 'benchmark',
            'production', 'capacity', 'manufacture', 'bottleneck'  # Added keywords
        ]
        
        for keyword in bi_keywords:
            if keyword in query_lower:
                return False
        
        return True
    
    def _is_hybrid_query(self, user_query):
        query_lower = user_query.lower()
        db_keywords = ['products', 'sales', 'customers', 'inventory', 'materials']
        forecast_keywords = ['stock', 'trend', 'forecast', 'predict', 'market']
        return any(kw in query_lower for kw in db_keywords) and any(kw in query_lower for kw in forecast_keywords)
    
    def _handle_simple_query(self, user_query, output_format):
        try:
            print("Generating SQL from natural language query...")
            data = run_nl_query(user_query)
            if not data or (isinstance(data, dict) and 'error' in data):
                response = {
                    "success": False,
                    "query_type": "simple",
                    "message": data.get("message", "No data found for your query"),
                    "suggestions": data.get("suggestions", ["Try rephrasing your question", "Check if the data exists in the system"])
                }
                return self._format_response(response, output_format)
            
            # Log SQL and sample data
            print(f"Raw SQL Generated:\n{data.get('raw_sql', 'N/A')}")
            print(f"Final SQL Query:\n{data.get('final_sql', 'N/A')}")
            print(f"Query returned {data['count']} rows")
            if data['count'] > 0:
                print("Sample of returned data:")
                df = pd.DataFrame(data['data'])
                print(df.head().to_string())
            
            response = {
                "success": True,
                "query_type": "simple",
                "data": data.get("data", []),
                "metadata": {
                    "count": data.get("count", 0),
                    "columns": data.get("columns", []),
                    "query": user_query,
                    "execution_time": "< 1s"
                },
                "display_hints": {
                    "format": "table",
                    "sortable": True,
                    "exportable": True
                },
                "summary": data.get("summary", "Results found")
            }
            return self._format_response(response, output_format)
        except Exception as e:
            error_response = {
                "success": False,
                "query_type": "simple",
                "error": str(e),
                "message": "Query processing failed",
                "suggestions": ["Try a simpler query", "Check your connection"]
            }
        return self._format_response(error_response, output_format)
    
    def _handle_business_intelligence(self, user_query, output_format):
        try:
            print("Starting comprehensive business intelligence analysis...")
            
            # Step 1: Fetch internal data
            print("Generating SQL from natural language query...")
            db_result = run_nl_query(user_query)
            
            # Log SQL steps and sample data
            if db_result.get("success", False):
                print(f"Raw SQL Generated:\n{db_result.get('raw_sql', 'N/A')}")
                print(f"Final SQL Query:\n{db_result.get('final_sql', 'N/A')}")
                print(f"Query returned {db_result.get('count', 0)} rows")
                if db_result.get("count", 0) > 0:
                    print("Sample of returned data:")
                    df = pd.DataFrame(db_result["data"])
                    print(df.head().to_string())
            else:
                print(f"Database query failed: {db_result.get('message', 'Unknown error')}")
                print(f"Suggestions: {', '.join(db_result.get('suggestions', []))}")
            
            # Step 2: Analyze data
            print("Analyzing company fundamentals...")
            analyzer = OptimizedBusinessAnalyzer()
            bi_response = analyzer.analyze_business_query(user_query, db_data=db_result.get("data", []))
            
            # Add query metadata
            bi_response["query_type"] = "business_intelligence"
            bi_response["query"] = user_query
            bi_response["execution_time"] = "1-3s"
            bi_response["database_results"] = db_result
            
            return self._format_response(bi_response, output_format)
        except Exception as e:
            print(f"[ERROR] Business intelligence analysis failed: {e}")
            error_response = {
                "success": False,
                "query_type": "business_intelligence",
                "error": str(e),
                "message": "Business intelligence analysis failed",
                "suggestions": ["Try a simpler query", "Check data availability", "Verify table schema", "Ensure database connection"]
            }
            return self._format_response(error_response, output_format)
    
    def _handle_hybrid_query(self, user_query, output_format):
        try:
            print("Processing hybrid query: combining database and forecasting analysis...")
            
            # Step 1: Fetch internal database results
            print("Generating SQL from natural language query...")
            db_result = run_nl_query(user_query)
            
            # Log SQL steps and sample data
            if db_result.get("success", False):
                print(f"Raw SQL Generated:\n{db_result.get('raw_sql', 'N/A')}")
                print(f"Final SQL Query:\n{db_result.get('final_sql', 'N/A')}")
                print(f"Query returned {db_result.get('count', 0)} rows")
                if db_result.get("count", 0) > 0:
                    print("Sample of returned data:")
                    df = pd.DataFrame(db_result["data"])
                    print(df.head().to_string())
            else:
                print(f"Database query failed: {db_result.get('message', 'Unknown error')}")
                print(f"Suggestions: {', '.join(db_result.get('suggestions', []))}")
            
            # Step 2: Fetch external forecast
            print("Generating forecast for external data...")
            forecast_result = run_forecast_for_topic(user_query)
            
            # Log forecast details
            if forecast_result.get("success", False):
                print(f"Resolved stock symbols: {forecast_result.get('symbols', [])}")
                print(f"Forecast summary: {forecast_result.get('summary', 'N/A')}")
            else:
                print(f"Forecast failed: {forecast_result.get('message', 'Unknown error')}")
                print(f"Suggestions: {', '.join(forecast_result.get('suggestions', []))}")
            
            # Step 3: Combine results using business analyzer
            print("Analyzing company fundamentals...")
            analyzer = OptimizedBusinessAnalyzer()
            bi_response = analyzer.analyze_business_query(user_query, db_data=db_result.get("data", []), forecast_data=forecast_result)
            
            # Add query metadata
            bi_response["query_type"] = "hybrid"
            bi_response["query"] = user_query
            bi_response["execution_time"] = "2-4s"
            bi_response["database_results"] = db_result
            bi_response["forecast"] = forecast_result.get("summary", "No forecast generated")
            
            return self._format_response(bi_response, output_format)
        except Exception as e:
            print(f"[ERROR] Hybrid query processing failed: {e}")
            error_response = {
                "success": False,
                "query_type": "hybrid",
                "error": str(e),
                "message": "Hybrid query processing failed",
                "suggestions": ["Try a simpler query", "Check data availability", "Verify stock symbols", "Ensure API keys are set"]
            }
            return self._format_response(error_response, output_format)
    
    def _format_response(self, response, output_format):
        if output_format == "json":
            return response
        
        if not response.get("success", False):
            return f"## Error\n{response.get('message', 'Unknown error')}\n\n**Suggestions:**\n" + \
                   "\n".join([f"- {s}" for s in response.get('suggestions', [])])
        
        if response.get("query_type") == "simple":
            return self._format_simple_response_text(response)
        elif response.get("query_type") == "hybrid":
            return self._format_hybrid_response_text(response)
        else:
            return self._format_bi_response_text(response)
    
    def _format_simple_response_text(self, response):
        text = f"## Query Results\n\n**Summary:** {response.get('summary', 'Results found')}\n\n"
        
        if response.get("data"):
            text += f"```json\n{json.dumps(response['data'], indent=2)}\n```\n\n"
        
        if response.get("metadata"):
            meta = response["metadata"]
            text += f"**Metadata:**\n"
            text += f"- Count: {meta.get('count', 'Unknown')}\n"
            text += f"- Columns: {', '.join(meta.get('columns', []))}\n"
            text += f"- Execution time: {meta.get('execution_time', 'Unknown')}\n"
        
        # Add visualizations if available
        if response.get("charts"):
            text += "## AVAILABLE VISUALIZATIONS\n\n"
            for chart in response["charts"]:
                text += f"- *{chart['title']}* ({chart['type']} chart)\n"
            text += "\n"
        
        return text
    
    def _format_hybrid_response_text(self, response):
        text = f"## Hybrid Query Results\n\n**Summary:** {response.get('summary', 'Results found')}\n\n"
        
        if response.get("database_results"):
            text += f"### Database Results\n```json\n{json.dumps(response['database_results'], indent=2)}\n```\n\n"
        
        if response.get("forecast"):
            text += f"### Forecast Analysis\n{response['forecast']}\n\n"
        
        if response.get("metadata"):
            meta = response["metadata"]
            text += f"**Metadata:**\n"
            text += f"- Count: {meta.get('count', 'Unknown')}\n"
            text += f"- Columns: {', '.join(meta.get('columns', []))}\n"
            text += f"- Execution time: {meta.get('execution_time', 'Unknown')}\n"
        
        # Add visualizations if available
        if response.get("charts"):
            text += "## AVAILABLE VISUALIZATIONS\n\n"
            for chart in response["charts"]:
                text += f"- *{chart['title']}* ({chart['type']} chart)\n"
            text += "\n"
        
        return text
    
    def _format_bi_response_text(self, response):
        text = "# COMPREHENSIVE BUSINESS INTELLIGENCE REPORT\n\n"
        
        # Executive Summary
        text += f"## EXECUTIVE SUMMARY\n\n*{response.get('summary', 'Analysis completed')}*\n\n"
        if response.get("key_points"):
            text += "**Key Points:**\n"
            for point in response["key_points"]:
                text += f"- {point}\n"
            text += "\n"
        
        # Key Performance Indicators
        if response.get("kpis"):
            text += "## KEY PERFORMANCE INDICATORS\n\n"
            for kpi in response["kpis"]:
                text += f"*{kpi['name']}:* {kpi['value']}\n"
            text += "\n"
        
        # Alerts
        if response.get("alerts"):
            text += "## ALERTS\n\n"
            for alert in response["alerts"]:
                text += f"*{alert['severity']}:* {alert['message']}\n"
            text += "\n"
        
        # Recommendations
        if response.get("recommendations"):
            text += "## RECOMMENDATIONS\n\n"
            for i, rec in enumerate(response["recommendations"], 1):
                text += f"*{i}. {rec['action']}* ({rec['priority']} priority)\n"
                text += f"   - {rec['description']}\n"
                text += f"   - Timeline: {rec['timeline']}\n"
                text += f"   - Impact: {rec['impact']}\n"
            text += "\n"
        
        # Visualizations
        if response.get("charts"):
            text += "## AVAILABLE VISUALIZATIONS\n\n"
            for chart in response["charts"]:
                text += f"- *{chart['title']}* ({chart['type']} chart)\n"
            text += "\n"
        
        # Additional Data (for hybrid queries)
        if response.get("query_type") == "hybrid" and response.get("forecast"):
            text += f"## FORECAST ANALYSIS\n\n{response['forecast']}\n\n"
        
        text += f"*Analysis Confidence:* {response.get('confidence', 'N/A')}%\n\n"
        text += "---\n*This analysis combines your company's internal data with market intelligence.*"
        
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