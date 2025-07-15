import json
import re
from nlp_sql import run_nl_query
from business_analyzer import OptimizedBusinessAnalyzer

# Security Configuration 
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
        """
        Unified query processor that supports both JSON and text output
        
        Args:
            user_query (str): The user's natural language query
            output_format (str): "json" for structured data, "text" for formatted text
        
        Returns:
            dict or str: Structured response or formatted text based on output_format
        """
        # Security validation
        security_check = self._validate_security(user_query)
        if not security_check["is_safe"]:
            return self._format_response(security_check, output_format)
        
        # Route based on query complexity
        if self._is_simple_query(user_query):
            return self._handle_simple_query(user_query, output_format)
        else:
            return self._handle_business_intelligence(user_query, output_format)
    
    def _validate_security(self, user_query):
        """Enhanced security validation with detailed feedback"""
        query_lower = user_query.lower()
        
        # Check forbidden phrases with context
        for phrase in FORBIDDEN_PHRASES:
            if phrase in query_lower:
                if any(context in query_lower for context in ['data', 'record', 'table', 'database', 'entry', 'row']):
                    return {
                        "is_safe": False,
                        "error_type": "DESTRUCTIVE_OPERATION",
                        "message": f"Destructive operation '{phrase}' detected",
                        "suggestion": "Try rephrasing your query to focus on reading data instead"
                    }
        
        # Check SQL keywords
        for keyword in FORBIDDEN_SQL_KEYWORDS:
            if keyword.lower() in query_lower:
                return {
                    "is_safe": False,
                    "error_type": "FORBIDDEN_SQL",
                    "message": f"SQL keyword '{keyword}' is not allowed",
                    "suggestion": "Use natural language instead of SQL commands"
                }
        
        # Check suspicious patterns
        suspicious_patterns = [
            (r'\b(delete|drop|truncate|alter|update|insert)\s+\w+', "SQL injection attempt"),
            (r';\s*(delete|drop|truncate|alter|update|insert)', "Command chaining detected"),
            (r'(--|\#|\/\*)', "SQL comment injection"),
            (r'union\s+select', "SQL union injection"),
            (r'exec\s*\(', "Code execution attempt"),
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
        """Enhanced simple query detection"""
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
        
        # Check for simple patterns
        for pattern in simple_patterns:
            if re.search(pattern, query_lower):
                return True
        
        # Check for business intelligence keywords
        bi_keywords = [
            'recommend', 'suggest', 'should', 'strategy', 'improve', 'optimize',
            'invest', 'opportunity', 'growth', 'profit', 'market', 'competitive',
            'forecast', 'predict', 'trend', 'analysis', 'insight', 'advice',
            'opportunity', 'risk', 'performance', 'compare', 'benchmark'
        ]
        
        for keyword in bi_keywords:
            if keyword in query_lower:
                return False
        
        return True
    
    def _handle_simple_query(self, user_query, output_format):
        """Handle simple queries with enhanced formatting"""
        try:
            data = run_nl_query(user_query)
            
            if not data or (isinstance(data, dict) and 'error' in data):
                response = {
                    "success": False,
                    "query_type": "simple",
                    "message": "No data found for your query",
                    "suggestions": [
                        "Try rephrasing your question",
                        "Check if the data exists in the system",
                        "Use more specific terms"
                    ]
                }
                return self._format_response(response, output_format)
            
            # Enhanced response structure
            response = {
                "success": True,
                "query_type": "simple",
                "data": data,
                "metadata": {
                    "count": len(data) if isinstance(data, list) else 1,
                    "columns": list(data[0].keys()) if data and isinstance(data, list) else [],
                    "query": user_query,
                    "execution_time": "< 1s"
                },
                "display_hints": {
                    "format": "table",
                    "sortable": True,
                    "exportable": True
                },
                "summary": f"Found {len(data) if isinstance(data, list) else 1} results for your query"
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
        """Handle complex BI queries with progress tracking"""
        try:
            if output_format == "text":
                print("Starting comprehensive business intelligence analysis...")
                print("Step 1: Analyzing company fundamentals...")
            
            # Use the optimized BI analyzer
            bi_response = self.bi_analyzer.analyze_business_query(user_query)
            
            # Add query metadata
            bi_response["query_type"] = "business_intelligence"
            bi_response["query"] = user_query
            bi_response["execution_time"] = "3-5s"
            
            return self._format_response(bi_response, output_format)
            
        except Exception as e:
            error_response = {
                "success": False,
                "query_type": "business_intelligence",
                "error": str(e),
                "message": "Business intelligence analysis failed",
                "fallback_suggestions": [
                    "Try a simpler question first",
                    "Check if your data is available",
                    "Contact support if the issue persists"
                ]
            }
            return self._format_response(error_response, output_format)
    
    def _format_response(self, response, output_format):
        """Format response based on requested output format"""
        if output_format == "json":
            return response
        
        # Convert to text format for CLI/legacy compatibility
        if not response.get("success", False):
            return f"## Error\n{response.get('message', 'Unknown error')}\n\n**Suggestions:**\n" + \
                   "\n".join([f"- {s}" for s in response.get('suggestions', [])])
        
        if response.get("query_type") == "simple":
            return self._format_simple_response_text(response)
        else:
            return self._format_bi_response_text(response)
    
    def _format_simple_response_text(self, response):
        """Format simple query response as text"""
        text = f"## Query Results\n\n**Summary:** {response.get('summary', 'Results found')}\n\n"
        
        if response.get("data"):
            text += f"```json\n{json.dumps(response['data'], indent=2)}\n```\n\n"
        
        if response.get("metadata"):
            meta = response["metadata"]
            text += f"**Metadata:**\n"
            text += f"- Count: {meta.get('count', 'Unknown')}\n"
            text += f"- Columns: {', '.join(meta.get('columns', []))}\n"
            text += f"- Execution time: {meta.get('execution_time', 'Unknown')}\n"
        
        return text
    
    def _format_bi_response_text(self, response):
        """Format BI response as text (similar to original main_router.py)"""
        if not response.get("success"):
            return f"## Business Intelligence Analysis Failed\n{response.get('message', 'Unknown error')}"
        
        text = "# COMPREHENSIVE BUSINESS INTELLIGENCE REPORT\n\n"
        
        # Executive Summary
        if response.get("summary"):
            summary = response["summary"]
            text += f"## EXECUTIVE SUMMARY\n\n"
            text += f"**{summary.get('headline', 'Business Analysis Complete')}**\n\n"
            
            if summary.get("key_points"):
                text += "**Key Points:**\n"
                for point in summary["key_points"]:
                    text += f"- {point}\n"
                text += "\n"
        
        # KPIs
        if response.get("kpis"):
            text += "## KEY PERFORMANCE INDICATORS\n\n"
            for kpi in response["kpis"]:
                text += f"**{kpi['name']}:** {kpi['value']} ({kpi.get('change', 'no change')})\n"
            text += "\n"
        
        # Alerts
        if response.get("alerts"):
            text += "## ALERTS\n\n"
            for alert in response["alerts"]:
                text += f"**{alert['type'].upper()}:** {alert['message']} - {alert['action']}\n"
            text += "\n"
        
        # Recommendations
        if response.get("recommendations"):
            text += "## RECOMMENDATIONS\n\n"
            for i, rec in enumerate(response["recommendations"], 1):
                text += f"**{i}. {rec['title']}** ({rec['priority']} priority)\n"
                text += f"   - {rec['description']}\n"
                text += f"   - Timeline: {rec.get('timeline', 'Not specified')}\n"
                text += f"   - Impact: {rec.get('impact', 'Unknown')}\n\n"
        
        # Charts info
        if response.get("charts"):
            text += "## AVAILABLE VISUALIZATIONS\n\n"
            for chart in response["charts"]:
                text += f"- **{chart['title']}** ({chart['type']} chart)\n"
            text += "\n"
        
        # Confidence score
        if response.get("confidence_score"):
            conf = response["confidence_score"]
            text += f"**Analysis Confidence:** {conf['level'].upper()} ({conf['score']:.1f}%)\n\n"
        
        text += "---\n*This analysis combines your company's internal data with market intelligence.*"
        
        return text

# Main routing function (backward compatible)
def route_query(user_query):
    """Backward compatible routing function"""
    router = UnifiedBusinessRouter()
    return router.process_query(user_query, output_format="text")

def route_query_json(user_query):
    """New JSON routing function for modern frontends"""
    router = UnifiedBusinessRouter()
    return router.process_query(user_query, output_format="json")

# Enhanced CLI interface
if __name__ == "__main__":
    print("\n🚀 AI ASSISTANT BUSINESS INTELLIGENCE SYSTEM")
    print("=" * 50)
    print("Features:")
    print("✅ Intelligent query routing")
    print("✅ Enhanced security validation")
    print("✅ Structured JSON responses")
    print("✅ Executive dashboards")
    print("✅ Real-time KPIs")
    print("✅ Actionable recommendations")
    print("\nQuery Examples:")
    print("Simple: 'Show me top 5 products'")
    print("Complex: 'What should we invest in to improve profitability?'")
    print("=" * 50)
    
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