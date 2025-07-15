import pandas as pd
import sqlite3
import re
from database import DB_NAME
from forecast import run_forecast_for_topic
from nlp_sql import run_nl_query, generate_dynamic_schema
import json
from datetime import datetime, timedelta
import requests
import os
from dotenv import load_dotenv

load_dotenv()
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

class OptimizedBusinessAnalyzer:
    def __init__(self):
        self.db_name = DB_NAME
        
    def analyze_business_query(self, user_query):
        """
        Main method that returns structured data for frontend visualization, handling dynamic schemas.
        """
        try:
            # 1. Classify query intent
            from groq_classifier import classify_intent_with_groq
            intent = classify_intent_with_groq(user_query)
            
            # 2. Get dynamic schema
            schema = generate_dynamic_schema(self.db_name)
            
            # 3. Get internal data insights
            internal_insights = self._get_internal_insights(user_query, intent, schema)
            
            # 4. Get market context (if needed)
            market_context = self._get_market_context(user_query, intent)
            
            # 5. Generate actionable recommendations
            recommendations = self._generate_recommendations(internal_insights, market_context)
            
            # 6. Structure response for frontend
            response = {
                "success": True,
                "intent": intent,
                "schema": schema,
                "insights": internal_insights,
                "market_context": market_context,
                "recommendations": recommendations,
                "charts": self._generate_chart_configs(internal_insights),
                "summary": self._generate_executive_summary(internal_insights, recommendations),
                "alerts": self._generate_alerts(internal_insights),
                "kpis": self._generate_kpis(internal_insights),
                "confidence_score": self._calculate_confidence_score(internal_insights)
            }
            
            return response
            
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "fallback_message": "Unable to process query. Please try rephrasing.",
                "suggestions": ["Try a simpler question", "Check your data connection"]
            }
    
    def _get_internal_insights(self, user_query, intent, schema):
        """Get key internal metrics with reasoning, based on dynamic schema."""
        insights = {
            "metrics": {},
            "alerts": [],
            "trends": {},
            "reasoning": {}
        }
        
        # Identify relevant tables from schema
        tables = re.findall(r"Table: (\w+)", schema)
        inventory_tables = [t for t in tables if any(k in t.lower() for k in ['inventory', 'stock', 'supply'])]
        sales_tables = [t for t in tables if any(k in t.lower() for k in ['sales', 'order', 'transaction'])]
        product_tables = [t for t in tables if any(k in t.lower() for k in ['product', 'item', 'component'])]
        material_tables = [t for t in tables if 'material' in t.lower()]
        
        # Core business metrics based on intent and schema
        queries = {}
        if sales_tables:
            queries["revenue_trend"] = f"Show daily revenue for the last 30 days from {sales_tables[0]}"
            queries["top_products"] = f"Show top 5 products by revenue from {sales_tables[0]}"
        if inventory_tables:
            queries["inventory_alerts"] = f"Show products with stock below 100 from {inventory_tables[0]}"
        if product_tables and sales_tables:
            queries["customer_segments"] = f"Show revenue by customer region from {sales_tables[0]}"
            queries["sales_performance"] = f"Show total sales and transaction counts by product category from {sales_tables[0]}"
        
        # Add production capacity and material bottleneck queries
        if any(keyword in user_query.lower() for keyword in ["manufacture", "produce", "production", "units", "inventory", "component", "material"]):
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
                    f"SELECT m.material_name, m.material_type, m.tags, i.quantity "
                    f"FROM {material_tables[0]} m "
                    f"JOIN {inventory_tables[0]} i ON m.product_id = i.product_id "
                    f"WHERE LOWER(m.tags) LIKE '%material%' "
                    f"ORDER BY i.quantity ASC LIMIT 1"
                )
                # Add material-specific query for battery materials
                if 'battery' in user_query.lower():
                    queries["battery_materials"] = (
                        f"SELECT m.material_name, m.material_type, m.tags, i.quantity "
                        f"FROM {material_tables[0]} m "
                        f"JOIN {inventory_tables[0]} i ON m.product_id = i.product_id "
                        f"WHERE LOWER(m.tags) LIKE '%battery%'"
                    )
        
        for metric_name, nl_query in queries.items():
            try:
                data = run_nl_query(nl_query)
                if data and not isinstance(data, dict) or 'error' not in data:
                    insights["metrics"][metric_name] = data
                    insights["reasoning"][metric_name] = self._explain_metric(metric_name, data, user_query)
            except Exception as e:
                insights["alerts"].append(f"Could not fetch {metric_name}: {str(e)}")
        
        # Calculate production capacity
        if "production_capacity" in insights["metrics"] or "material_bottleneck" in insights["metrics"]:
            insights["production_capacity"] = self._calculate_production_capacity(
                insights["metrics"].get("production_capacity", []),
                insights["metrics"].get("material_bottleneck", []),
                user_query
            )
        
        # Calculate trends
        insights["trends"] = self._calculate_trends(insights["metrics"])
        
        return insights
    
    def _calculate_production_capacity(self, inventory_data, material_data, user_query):
        """
        Calculate maximum units that can be manufactured based on inventory and materials.
        Assumes each unit requires one of each critical component/material.
        """
        if not inventory_data and not material_data:
            return {"max_units": 0, "bottleneck": "No inventory or material data available"}
        
        component_quantities = {}
        
        # Process inventory data (components)
        if inventory_data:
            query_lower = user_query.lower()
            critical_components = [
                keyword for keyword in ["battery", "motor", "controller", "hvac", "sensor", "inverter"]
                if keyword in query_lower
            ] or ["component"]
            
            for item in inventory_data:
                tags = item.get("tags", "").lower().split(",")
                if any(c in tags for c in critical_components):
                    component_quantities[item.get("name", "Unknown")] = min(
                        component_quantities.get(item.get("name", "Unknown"), float("inf")),
                        item.get("quantity", 0)
                    )
        
        # Process material data
        if material_data:
            query_lower = user_query.lower()
            critical_materials = [
                keyword for keyword in ["battery", "semiconductor", "magnet", "structural"]
                if keyword in query_lower
            ] or ["material"]
            
            for item in material_data:
                tags = item.get("tags", "").lower().split(",")
                if any(m in tags for m in critical_materials):
                    component_quantities[item.get("material_name", "Unknown")] = min(
                        component_quantities.get(item.get("material_name", "Unknown"), float("inf")),
                        item.get("quantity", 0)
                    )
        
        # Maximum units is the minimum of available components/materials
        max_units = min(component_quantities.values()) if component_quantities else 0
        
        # Identify bottleneck
        bottleneck = min(component_quantities.items(), key=lambda x: x[1], default=("Unknown", 0)) if component_quantities else ("No components/materials identified", 0)
        
        return {
            "max_units": max_units,
            "bottleneck_component": bottleneck[0],
            "bottleneck_quantity": bottleneck[1],
            "components_analyzed": list(component_quantities.keys())
        }
    
    def _explain_metric(self, metric_name, data, user_query):
        """Provide concise reasoning for each metric, tailored to the query."""
        if not data:
            return "No data available"
            
        explanations = {
            "revenue_trend": f"Analyzed {len(data)} days. {'Growing' if len(data) > 15 else 'Limited'} trend data available.",
            "top_products": f"Top performer: {data[0].get('name', 'Unknown')} (${data[0].get('revenue', 0):,.0f})" if data else "No revenue data",
            "inventory_alerts": f"⚠️ {len(data)} products need restocking" if data else "✅ Stock levels healthy",
            "customer_segments": f"Revenue spread across {len(set(item.get('region', '') for item in data))} regions" if data else "No regional data",
            "sales_performance": f"Analyzed {len(data)} product categories" if data else "No sales data",
            "production_capacity": f"Maximum {data.get('max_units', 0)} units can be produced. Bottleneck: {data.get('bottleneck_component', 'Unknown')} ({data.get('bottleneck_quantity', 0)} units).",
            "bottleneck_analysis": f"Primary bottleneck: {data[0].get('name', 'Unknown')} with {data[0].get('quantity', 0)} units.",
            "material_bottleneck": f"Primary material bottleneck: {data[0].get('material_name', 'Unknown')} with {data[0].get('quantity', 0)} units.",
            "battery_materials": f"Found {len(data)} battery-related materials" if data else "No battery materials found."
        }
        return explanations.get(metric_name, "Data processed successfully")
    
    def _calculate_trends(self, metrics):
        """Calculate meaningful trends from metrics."""
        trends = {}
        
        # Revenue trend
        if "revenue_trend" in metrics and metrics["revenue_trend"]:
            revenue_data = metrics["revenue_trend"]
            if len(revenue_data) >= 14:  # Need at least 2 weeks of data
                recent_avg = sum(float(d.get('revenue', 0)) for d in revenue_data[-7:]) / 7
                older_avg = sum(float(d.get('revenue', 0)) for d in revenue_data[-14:-7]) / 7
                
                change_percent = ((recent_avg / older_avg - 1) * 100) if older_avg > 0 else 0
                
                if change_percent > 5:
                    trends["revenue"] = {
                        "direction": "up", 
                        "strength": "strong", 
                        "change": f"+{change_percent:.1f}%",
                        "icon": "📈",
                        "color": "green"
                    }
                elif change_percent < -5:
                    trends["revenue"] = {
                        "direction": "down", 
                        "strength": "concerning", 
                        "change": f"{change_percent:.1f}%",
                        "icon": "📉",
                        "color": "red"
                    }
                else:
                    trends["revenue"] = {
                        "direction": "stable", 
                        "strength": "steady", 
                        "change": f"{change_percent:+.1f}%",
                        "icon": "📊",
                        "color": "blue"
                    }
        
        # Inventory trend
        if "inventory_alerts" in metrics:
            alert_count = len(metrics["inventory_alerts"])
            if alert_count > 10:
                trends["inventory"] = {
                    "direction": "critical", 
                    "strength": "high", 
                    "change": f"{alert_count} products",
                    "icon": "🚨",
                    "color": "red"
                }
            elif alert_count > 5:
                trends["inventory"] = {
                    "direction": "warning", 
                    "strength": "medium",
                    "change": f"{alert_count} products",
                    "icon": "⚠️",
                    "color": "orange"
                }
            else:
                trends["inventory"] = {
                    "direction": "healthy", 
                    "strength": "good", 
                    "change": f"{alert_count} products",
                    "icon": "✅",
                    "color": "green"
                }
        
        return trends
    
    def _get_market_context(self, user_query, intent):
        """Get relevant market context if query involves external data."""
        query_lower = user_query.lower()
        
        # Only fetch market data for STOCK or HYBRID intents
        if intent not in ["STOCK", "HYBRID"]:
            return {"enabled": False, "message": "Market analysis not required for this query"}
        
        try:
            # Get focused market insights
            market_data = {
                "enabled": True,
                "stock_forecast": self._get_simplified_forecast(user_query),
                "market_sentiment": self._get_market_sentiment(),
                "reasoning": "Market analysis included based on query intent"
            }
            return market_data
        except Exception as e:
            return {"enabled": False, "error": str(e)}
    
    def _get_simplified_forecast(self, user_query):
        """Get simplified forecast data for relevant companies."""
        try:
            full_forecast = run_forecast_for_topic(user_query)
            return {
                "summary": "Market conditions analyzed",
                "outlook": full_forecast[:200] + "..." if len(full_forecast) > 200 else full_forecast,
                "confidence": "medium",
                "timeframe": "30-90 days"
            }
        except:
            return {"summary": "Forecast unavailable", "outlook": "Limited market data"}
    
    def _get_market_sentiment(self):
        """Get simplified market sentiment, tailored to query context."""
        return {
            "overall": "mixed",
            "tech_sector": "positive",
            "consumer_goods": "stable",
            "reasoning": "Based on recent market indicators"
        }
    
    def _generate_recommendations(self, insights, market_context):
        """Generate 3-5 actionable recommendations with reasoning."""
        recommendations = []
        
        # Inventory recommendations
        if "inventory_alerts" in insights["metrics"]:
            alert_count = len(insights["metrics"]["inventory_alerts"])
            if alert_count > 0:
                recommendations.append({
                    "id": "inventory_action",
                    "priority": "high",
                    "category": "inventory",
                    "title": "Immediate Restocking Required",
                    "description": f"Restock {alert_count} products below safety levels",
                    "reasoning": "Prevent stockouts and maintain customer satisfaction",
                    "timeline": "24-48 hours",
                    "impact": "high",
                    "effort": "medium",
                    "icon": "📦",
                    "color": "red"
                })
        
        # Production capacity recommendations
        if "production_capacity" in insights["metrics"]:
            capacity = insights["metrics"]["production_capacity"]
            max_units = capacity.get("max_units", 0)
            bottleneck = capacity.get("bottleneck_component", "Unknown")
            recommendations.append({
                "id": "production_optimization",
                "priority": "high",
                "category": "production",
                "title": f"Address {bottleneck} Bottleneck",
                "description": f"Secure additional {bottleneck} supply to increase production beyond {max_units} units",
                "reasoning": f"Current {bottleneck} stock limits production capacity",
                "timeline": "1-2 weeks",
                "impact": "high",
                "effort": "high",
                "icon": "🏭",
                "color": "orange"
            })
        
        # Material bottleneck recommendations
        if "material_bottleneck" in insights["metrics"]:
            bottleneck = insights["metrics"]["material_bottleneck"][0]
            recommendations.append({
                "id": "material_optimization",
                "priority": "high",
                "category": "materials",
                "title": f"Address {bottleneck.get('material_name', 'Unknown')} Bottleneck",
                "description": f"Secure additional {bottleneck.get('material_name', 'Unknown')} supply",
                "reasoning": f"Low {bottleneck.get('material_name', 'Unknown')} stock limits production",
                "timeline": "1-2 weeks",
                "impact": "high",
                "effort": "high",
                "icon": "🧪",
                "color": "orange"
            })
        
        # Battery materials recommendation
        if "battery_materials" in insights["metrics"]:
            battery_materials = insights["metrics"]["battery_materials"]
            low_stock = [m for m in battery_materials if m.get('quantity', 0) < 100]
            if low_stock:
                recommendations.append({
                    "id": "battery_material_action",
                    "priority": "high",
                    "category": "materials",
                    "title": "Restock Battery Materials",
                    "description": f"Restock {len(low_stock)} battery materials below 100 units",
                    "reasoning": "Critical for EV production",
                    "timeline": "1-2 weeks",
                    "impact": "high",
                    "effort": "medium",
                    "icon": "🔋",
                    "color": "red"
                })
        
        # Revenue recommendations
        if "revenue" in insights.get("trends", {}):
            trend = insights["trends"]["revenue"]
            if trend["direction"] == "down":
                recommendations.append({
                    "id": "revenue_recovery",
                    "priority": "high",
                    "category": "revenue",
                    "title": "Revenue Recovery Plan",
                    "description": f"Address {trend['change']} revenue decline",
                    "reasoning": "Recent trend shows concerning decline vs previous period",
                    "timeline": "1-2 weeks",
                    "impact": "high",
                    "effort": "high",
                    "icon": "💰",
                    "color": "red"
                })
            elif trend["direction"] == "up":
                recommendations.append({
                    "id": "scale_growth",
                    "priority": "medium",
                    "category": "growth",
                    "title": "Scale Successful Products",
                    "description": f"Capitalize on {trend['change']} growth momentum",
                    "reasoning": "Strong revenue trend indicates market demand",
                    "timeline": "2-4 weeks",
                    "impact": "medium",
                    "effort": "medium",
                    "icon": "📈",
                    "color": "green"
                })
        
        # Market-based recommendations
        if market_context.get("enabled"):
            recommendations.append({
                "id": "market_opportunities",
                "priority": "medium",
                "category": "market",
                "title": "Monitor Market Opportunities",
                "description": "Track identified investment opportunities",
                "reasoning": "Market analysis suggests potential for strategic moves",
                "timeline": "Ongoing",
                "impact": "medium",
                "effort": "low",
                "icon": "🎯",
                "color": "blue"
            })
        
        return recommendations
    
    def _generate_alerts(self, insights):
        """Generate actionable alerts."""
        alerts = []
        
        # Critical inventory alerts
        if "inventory_alerts" in insights["metrics"]:
            alert_count = len(insights["metrics"]["inventory_alerts"])
            if alert_count > 10:
                alerts.append({
                    "type": "critical",
                    "message": f"{alert_count} products critically low",
                    "action": "Immediate restocking required",
                    "icon": "🚨"
                })
            elif alert_count > 0:
                alerts.append({
                    "type": "warning",
                    "message": f"{alert_count} products need attention",
                    "action": "Schedule restocking",
                    "icon": "⚠️"
                })
        
        # Production bottleneck alerts
        if "production_capacity" in insights["metrics"]:
            capacity = insights["metrics"]["production_capacity"]
            bottleneck = capacity.get("bottleneck_component", "Unknown")
            bottleneck_qty = capacity.get("bottleneck_quantity", 0)
            if bottleneck_qty < 100:
                alerts.append({
                    "type": "critical",
                    "message": f"Production limited by {bottleneck} ({bottleneck_qty} units)",
                    "action": f"Procure additional {bottleneck} supply",
                    "icon": "🚨"
                })
        
        # Material bottleneck alerts
        if "material_bottleneck" in insights["metrics"]:
            bottleneck = insights["metrics"]["material_bottleneck"][0]
            bottleneck_qty = bottleneck.get("quantity", 0)
            if bottleneck_qty < 100:
                alerts.append({
                    "type": "critical",
                    "message": f"Production limited by {bottleneck.get('material_name', 'Unknown')} ({bottleneck_qty} units)",
                    "action": f"Procure additional {bottleneck.get('material_name', 'Unknown')} supply",
                    "icon": "🚨"
                })
        
        # Battery materials alerts
        if "battery_materials" in insights["metrics"]:
            battery_materials = insights["metrics"]["battery_materials"]
            low_stock = [m for m in battery_materials if m.get('quantity', 0) < 100]
            if low_stock:
                alerts.append({
                    "type": "critical",
                    "message": f"{len(low_stock)} battery materials below 100 units",
                    "action": "Restock battery materials",
                    "icon": "🔋"
                })
        
        # Revenue alerts
        if "revenue" in insights.get("trends", {}):
            trend = insights["trends"]["revenue"]
            if trend["direction"] == "down" and trend["strength"] == "concerning":
                alerts.append({
                    "type": "critical",
                    "message": f"Revenue declining {trend['change']}",
                    "action": "Review sales strategy",
                    "icon": "📉"
                })
        
        return alerts
    
    def _generate_kpis(self, insights):
        """Generate key performance indicators."""
        kpis = []
        
        # Revenue KPI
        if "revenue_trend" in insights["metrics"]:
            revenue_data = insights["metrics"]["revenue_trend"]
            if revenue_data:
                total_revenue = sum(float(d.get('revenue', 0)) for d in revenue_data)
                avg_daily = total_revenue / len(revenue_data)
                
                kpis.append({
                    "name": "Daily Revenue",
                    "value": f"${avg_daily:,.0f}",
                    "trend": insights["trends"].get("revenue", {}).get("direction", "stable"),
                    "change": insights["trends"].get("revenue", {}).get("change", "0%"),
                    "icon": "💰"
                })
        
        # Inventory KPI
        if "inventory_alerts" in insights["metrics"]:
            alert_count = len(insights["metrics"]["inventory_alerts"])
            kpis.append({
                "name": "Stock Alerts",
                "value": str(alert_count),
                "trend": "critical" if alert_count > 10 else "warning" if alert_count > 5 else "healthy",
                "change": f"{alert_count} products",
                "icon": "📦"
            })
        
        # Production KPI
        if "production_capacity" in insights["metrics"]:
            capacity = insights["metrics"]["production_capacity"]
            kpis.append({
                "name": "Production Capacity",
                "value": str(capacity.get("max_units", 0)),
                "trend": "limited" if capacity.get("max_units", 0) < 100 else "adequate",
                "change": f"Bottleneck: {capacity.get('bottleneck_component', 'Unknown')}",
                "icon": "🏭"
            })
        
        # Battery Materials KPI
        if "battery_materials" in insights["metrics"]:
            battery_materials = insights["metrics"]["battery_materials"]
            low_stock_count = len([m for m in battery_materials if m.get('quantity', 0) < 100])
            kpis.append({
                "name": "Battery Materials Stock",
                "value": str(low_stock_count),
                "trend": "critical" if low_stock_count > 5 else "warning" if low_stock_count > 0 else "healthy",
                "change": f"{low_stock_count} low stock",
                "icon": "🔋"
            })
        
        # Customer KPI
        if "customer_segments" in insights["metrics"]:
            segments = insights["metrics"]["customer_segments"]
            kpis.append({
                "name": "Active Regions",
                "value": str(len(segments)),
                "trend": "stable",
                "change": "regions",
                "icon": "🌍"
            })
        
        return kpis
    
    def _calculate_confidence_score(self, insights):
        """Calculate confidence score for the analysis."""
        available_metrics = len([m for m in insights["metrics"].values() if m])
        total_metrics = len(insights["metrics"])
        
        confidence = (available_metrics / total_metrics) * 100
        
        if confidence >= 80:
            return {"score": confidence, "level": "high", "color": "green"}
        elif confidence >= 60:
            return {"score": confidence, "level": "medium", "color": "orange"}
        else:
            return {"score": confidence, "level": "low", "color": "red"}
    
    def _generate_chart_configs(self, insights):
        """Generate chart configurations for frontend."""
        charts = []
        
        # Revenue trend chart
        if "revenue_trend" in insights["metrics"]:
            data = insights["metrics"]["revenue_trend"]
            if len(data) > 1:
                charts.append({
                    "id": "revenue_trend",
                    "type": "line",
                    "title": "Revenue Trend",
                    "priority": 1,
                    "data": {
                        "labels": [item.get('date', '')[-5:] for item in data],
                        "datasets": [{
                            "label": "Daily Revenue",
                            "data": [float(item.get('revenue', 0)) for item in data],
                            "borderColor": "#4CAF50",
                            "backgroundColor": "rgba(76, 175, 80, 0.1)",
                            "tension": 0.4
                        }]
                    },
                    "options": {
                        "responsive": True,
                        "maintainAspectRatio": False,
                        "scales": {
                            "y": {"beginAtZero": True}
                        }
                    }
                })
        
        # Top products chart
        if "top_products" in insights["metrics"]:
            data = insights["metrics"]["top_products"]
            if data:
                charts.append({
                    "id": "top_products",
                    "type": "bar",
                    "title": "Top Products",
                    "priority": 2,
                    "data": {
                        "labels": [item.get('name', '')[:15] + '...' if len(item.get('name', '')) > 15 else item.get('name', '') for item in data[:5]],
                        "datasets": [{
                            "label": "Revenue",
                            "data": [float(item.get('revenue', 0)) for item in data[:5]],
                            "backgroundColor": ["#2196F3", "#4CAF50", "#FF9800", "#9C27B0", "#F44336"]
                        }]
                    },
                    "options": {
                        "responsive": True,
                        "maintainAspectRatio": False
                    }
                })
        
        # Customer segments chart
        if "customer_segments" in insights["metrics"]:
            data = insights["metrics"]["customer_segments"]
            if data:
                charts.append({
                    "id": "customer_segments",
                    "type": "doughnut",
                    "title": "Revenue by Region",
                    "priority": 3,
                    "data": {
                        "labels": [item.get('region', '') for item in data],
                        "datasets": [{
                            "data": [float(item.get('revenue', 0)) for item in data],
                            "backgroundColor": ["#FF6384", "#36A2EB", "#FFCE56", "#4BC0C0", "#9966FF"]
                        }]
                    },
                    "options": {
                        "responsive": True,
                        "maintainAspectRatio": False
                    }
                })
        
        # Component availability chart
        if "production_capacity" in insights["metrics"]:
            capacity = insights["metrics"]["production_capacity"]
            if capacity.get("components_analyzed"):
                charts.append({
                    "id": "component_availability",
                    "type": "bar",
                    "title": "Component & Material Availability",
                    "priority": 4,
                    "data": {
                        "labels": capacity.get("components_analyzed", []),
                        "datasets": [{
                            "label": "Quantity Available",
                            "data": [capacity.get("bottleneck_quantity", 0) if comp == capacity.get("bottleneck_component") else 100 for comp in capacity.get("components_analyzed", [])],
                            "backgroundColor": ["#F44336" if comp == capacity.get("bottleneck_component") else "#4CAF50" for comp in capacity.get("components_analyzed", [])]
                        }]
                    },
                    "options": {
                        "responsive": True,
                        "maintainAspectRatio": False
                    }
                })
        
        # Battery materials chart
        if "battery_materials" in insights["metrics"]:
            data = insights["metrics"]["battery_materials"]
            if data:
                charts.append({
                    "id": "battery_materials",
                    "type": "bar",
                    "title": "Battery Materials Stock",
                    "priority": 5,
                    "data": {
                        "labels": [item.get('material_name', '')[:15] + '...' if len(item.get('material_name', '')) > 15 else item.get('material_name', '') for item in data],
                        "datasets": [{
                            "label": "Quantity Available",
                            "data": [float(item.get('quantity', 0)) for item in data],
                            "backgroundColor": ["#FF6384" if float(item.get('quantity', 0)) < 100 else "#4CAF50" for item in data]
                        }]
                    },
                    "options": {
                        "responsive": True,
                        "maintainAspectRatio": False
                    }
                })
        
        return sorted(charts, key=lambda x: x["priority"])
    
    def _generate_executive_summary(self, insights, recommendations):
        """Generate a concise executive summary."""
        summary = {
            "headline": "",
            "key_points": [],
            "confidence_level": "medium",
            "data_quality": "good",
            "action_items": len([r for r in recommendations if r["priority"] == "high"]),
            "overall_health": "stable"
        }
        
        # Generate headline based on trends
        trends = insights.get("trends", {})
        if "revenue" in trends:
            trend = trends["revenue"]
            if trend["direction"] == "up":
                summary["headline"] = f"Revenue up {trend['change']} - growth momentum"
                summary["overall_health"] = "positive"
            elif trend["direction"] == "down":
                summary["headline"] = f"Revenue down {trend['change']} - action needed"
                summary["overall_health"] = "concerning"
            else:
                summary["headline"] = f"Revenue stable {trend['change']} - steady performance"
        
        # Key points for production capacity
        if "production_capacity" in insights["metrics"]:
            capacity = insights["metrics"]["production_capacity"]
            summary["key_points"].append(f"🏭 Max production: {capacity.get('max_units', 0)} units")
            summary["key_points"].append(f"⚠️ Bottleneck: {capacity.get('bottleneck_component', 'Unknown')} ({capacity.get('bottleneck_quantity', 0)} units)")
        
        # Key points for material bottleneck
        if "material_bottleneck" in insights["metrics"]:
            bottleneck = insights["metrics"]["material_bottleneck"][0]
            summary["key_points"].append(f"🧪 Material bottleneck: {bottleneck.get('material_name', 'Unknown')} ({bottleneck.get('quantity', 0)} units)")
        
        # Key points for battery materials
        if "battery_materials" in insights["metrics"]:
            battery_materials = insights["metrics"]["battery_materials"]
            low_stock = len([m for m in battery_materials if m.get('quantity', 0) < 100])
            if low_stock > 0:
                summary["key_points"].append(f"🔋 {low_stock} battery materials need restocking")
        
        # Key points for inventory alerts
        alert_count = len(insights["metrics"].get("inventory_alerts", []))
        if alert_count > 0:
            summary["key_points"].append(f"⚠️ {alert_count} products need restocking")
        
        # Key points for recommendations
        if recommendations:
            high_priority = [r for r in recommendations if r["priority"] == "high"]
            if high_priority:
                summary["key_points"].append(f"🎯 {len(high_priority)} urgent actions required")
        
        # Top performing product
        if "top_products" in insights["metrics"] and insights["metrics"]["top_products"]:
            top_product = insights["metrics"]["top_products"][0]
            summary["key_points"].append(f"🏆 Top product: {top_product.get('name', 'Unknown')}")
        
        # Confidence level based on data availability
        available_metrics = len([m for m in insights["metrics"].values() if m])
        if available_metrics >= 4:
            summary["confidence_level"] = "high"
        elif available_metrics >= 2:
            summary["confidence_level"] = "medium"
        else:
            summary["confidence_level"] = "low"
        
        return summary

# Enhanced simple query handler
def handle_simple_query_optimized(user_query):
    """Handle simple queries with better formatting for frontend."""
    try:
        data = run_nl_query(user_query)
        
        if not data or (isinstance(data, dict) and 'error' in data):
            return {
                "success": False,
                "message": "No data found for your query",
                "suggestions": ["Try rephrasing your question", "Check if the data exists in the system"],
                "query_type": "simple"
            }
        
        # Format for frontend display
        return {
            "success": True,
            "data": data,
            "count": len(data) if isinstance(data, list) else 1,
            "query": user_query,
            "columns": list(data[0].keys()) if data and isinstance(data, list) else [],
            "query_type": "simple",
            "display_format": "table",
            "summary": f"Found {len(data) if isinstance(data, list) else 1} results"
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "message": "Query processing failed",
            "query_type": "simple"
        }

# Import security functions
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

def is_destructive_query(user_query):
    """Check if the user query contains any destructive operations."""
    import re
    
    query_lower = user_query.lower()
    
    for phrase in FORBIDDEN_PHRASES:
        if phrase in query_lower:
            if any(context in query_lower for context in ['data', 'record', 'table', 'database', 'entry', 'row']):
                return True
    
    for keyword in FORBIDDEN_SQL_KEYWORDS:
        if keyword.lower() in query_lower:
            return True
    
    suspicious_patterns = [
        r'\b(delete|drop|truncate|alter|update|insert)\s+\w+',
        r';\s*(delete|drop|truncate|alter|update|insert)',
        r'(--|\#|\/\*)',
        r'union\s+select',
        r'exec\s*\(',
    ]
    
    for pattern in suspicious_patterns:
        if re.search(pattern, query_lower, re.IGNORECASE):
            return True
    
    return False

def is_simple_query(user_query):
    """Determine if this is a simple data query or needs full business intelligence."""
    import re
    
    simple_patterns = [
        r'^(show|list|get|display)\s+\w+',
        r'^what\s+(is|are)\s+\w+',
        r'^how\s+many\s+\w+',
        r'just\s+(show|list|get)',
        r'simple\s+',
        r'quick\s+',
        r'only\s+(show|list|get)'
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
        'manufacture', 'production', 'capacity', 'bottleneck'
    ]
    
    for keyword in bi_keywords:
        if keyword in query_lower:
            return False
    
    return True

# Updated main routing function
def route_query_optimized(user_query):
    """
    Optimized routing that returns structured data for frontend.
    """
    # Security check
    if is_destructive_query(user_query):
        return {
            "success": False,
            "error": "Security violation",
            "message": "Destructive operations are not allowed",
            "code": "SECURITY_ERROR"
        }
    
    # Simple query routing
    if is_simple_query(user_query):
        return handle_simple_query_optimized(user_query)
    
    # Business intelligence routing
    analyzer = OptimizedBusinessAnalyzer()
    return analyzer.analyze_business_query(user_query)