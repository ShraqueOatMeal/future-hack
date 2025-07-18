# KOWALSKI: AI-Powered NLP-to-SQL Data Insights Tool

## FutureHack Problem Statement

The challenge presented at FutureHack was to create an NLP system that converts plain-English questions into executable SQL or Python code, making data analysis accessible to non-coders. KOWALSKI not only meets this goal but goes far beyond, delivering an extravagant, feature-rich application that transforms the simplicity of the problem statement into a powerful, scalable, and user-friendly platform for data insights. Our goal was to innovate boldly, building a highly scalable and versatile tool that serves both small businesses and the largest corporations, redefining how users of all scales interact with their data.

## Project Overview

KOWALSKI is an innovative AI-powered tool designed to democratize data analysis by transforming natural language queries into SQL, enabling instant insights from company databases or uploaded CSV files. Built with scalability and versatility as core objectives, KOWALSKI empowers users, from small startups to global corporations, to explore their performance data intuitively. Users can connect directly to their company databases for seamless access or upload CSV files, then ask questions in plain English to receive actionable insights through dynamic SQL queries, visualizations, and predictive analytics powered by real-time stock data from the TwelveData API.

Developed for FutureHack, KOWALSKI combines cutting-edge natural language processing powered by the Grok 4 API, dynamic schema recognition, and predictive modeling to deliver a seamless, no-code data analysis experience. By building an extravagant application that far exceeds the basic requirements of the problem statement, we aimed to create a tool that scales effortlessly across diverse use cases, from a small business analyzing local sales to a multinational corporation forecasting global stock trends. KOWALSKI's ability to integrate with existing databases ensures easy usability, eliminating the need for repetitive file uploads and enabling real-time data access for enterprises of any size.

## Key Features

KOWALSKI is packed with features that streamline data analysis and provide powerful insights, elevating the core NLP-to-SQL concept into a robust, scalable platform:

### 1. NLP-to-SQL Engine
- What it does: Translates plain English into precise SQL queries in real time.
- How it works: Powered by the Grok 4 API, KOWALSKI parses user queries, maps them to the dataset's schema, and generates optimized SQL queries. For example, asking "What are my top-selling products this year?" results in a SQL query like `SELECT product, SUM(sales) FROM data WHERE year = 2025 GROUP BY product ORDER BY SUM(sales) DESC`.
- Why it matters: Eliminates the need for SQL expertise, making data analysis accessible to all, directly addressing the hackathon's goal of empowering non-coders, while scaling to handle complex queries for large corporations.

### 2. Dynamic Schema Support
- What it does: Automatically detects and adapts to the structure of company databases or uploaded CSV files.
- How it works: KOWALSKI analyzes column names, data types, and relationships within databases or CSVs to create a flexible schema. It handles diverse datasets, from sales records to inventory logs, without requiring manual configuration.
- Why it matters: Users can connect to their existing databases or upload CSVs and start querying immediately, ensuring versatility for small businesses and large enterprises alike.

### 3. Insightful Analysis
- What it does: Delivers instant visual and textual summaries of query results.
- How it works: After executing SQL queries, KOWALSKI generates charts (e.g., bar, line, pie) and concise textual insights to highlight trends, outliers, and key metrics.
- Why it matters: Visualizations make complex data easy to understand, enabling faster decision-making across organizations of any size, from startups to global corporations.

### 4. Stock Forecasting and Recommendations
- What it does: Provides predictive insights and actionable business recommendations based on dataset trends and real-time stock data.
- How it works: Using machine learning algorithms and real-time data from the TwelveData API, KOWALSKI analyzes historical patterns (e.g., sales or stock levels) to forecast future trends and suggest strategies like inventory restocking or pricing adjustments.
- Why it matters: This innovative addition goes beyond the problem statement, offering predictive analytics that scale from small business inventory planning to corporate stock market strategies.

### 5. No Setup Required
- What it does: Enables instant analysis with zero configuration for CSV uploads and seamless integration with company databases.
- How it works: Users can connect directly to their company databases for real-time data access or upload a CSV file, and KOWALSKI handles everything, from schema detection to query execution, without requiring technical knowledge.
- Why it matters: Lowers barriers to entry, making KOWALSKI ideal for startups, small businesses, and large corporations, aligning with our goal of scalability and easy usability.

## How KOWALSKI Works

KOWALSKI's architecture is designed for simplicity, scalability, and accuracy, building on the problem statement's core requirement while introducing advanced functionality to serve organizations of all sizes. Here's a high-level overview of its workflow:

1. Database Connection or CSV Upload:
   - Users connect to their company databases for direct data access or upload a CSV file containing their data (e.g., sales, inventory, or financial records).
   - KOWALSKI parses the database or file to infer column names, data types, and relationships, creating a dynamic schema.

2. Natural Language Query:
   - Users input questions in plain English, such as "What is the average revenue per region in 2025?" or "Which products had the highest returns?"
   - The Grok 4 API-powered NLP engine processes the query, identifying key entities (e.g., "revenue," "region") and intents (e.g., "average," "group by"), fulfilling the hackathon's NLP-to-code requirement.

3. SQL Generation:
   - The NLP-to-SQL engine, leveraging Grok 4 API, maps the query to the dataset's schema, generating an optimized SQL query.
   - Example: For "Show me total sales by product," the system might generate:
     ```sql
     SELECT product, SUM(sales) AS total_sales
     FROM data
     GROUP BY product
     ORDER BY total_sales DESC;
     ```

4. Query Execution and Analysis:
   - The generated SQL query is executed on the connected database or uploaded dataset.
   - Results are processed to produce visualizations (e.g., bar charts for sales trends) and textual summaries (e.g., "Product X had the highest sales at $10,000"), adding value beyond the problem statement.

5. Forecasting and Recommendations:
   - For forecasting queries, KOWALSKI applies predictive models to historical data and real-time stock data from the TwelveData API, generating insights like "Based on current trends, Product Y is likely to see a 15% sales increase next quarter."
   - Recommendations are derived from patterns, such as suggesting inventory adjustments based on stock levels, showcasing our innovative expansion of the original challenge.

6. Output Delivery:
   - Results are presented in an intuitive interface with visualizations, tables, and plain-language explanations.
   - Users can refine queries or ask follow-up questions to dive deeper into the data, ensuring scalability for complex enterprise needs.

## Use Cases

KOWALSKI's scalability and versatility make it suitable for a wide range of users, from small businesses to the largest corporations:

- Small Businesses: Analyze sales, customer, or inventory data without hiring a data analyst, leveraging easy database integration.
- Data Enthusiasts: Explore datasets quickly during hackathons or personal projects with CSV uploads.
- Startups: Gain insights into performance metrics to inform growth strategies with minimal setup.
- Large Corporations: Query massive company databases for real-time insights, leveraging TwelveData API for stock forecasting.
- Non-Technical Teams: Enable marketing, HR, or operations teams to query data without SQL knowledge.
- Stock Forecasting: Predict inventory needs or sales trends for better planning, using real-time stock data.

Example Queries:
- "What are my top 5 customers by revenue?"
- "How did sales perform month-over-month in 2025?"
- "Which regions are underperforming in Q3?"
- "What's the forecasted stock demand for next quarter based on real-time market data?"

## Limitations

While KOWALSKI is powerful, it has some limitations to consider:

1. Schema Misinterpretation:
   - Unclear or inconsistent column names in databases or CSVs (e.g., "sales" vs. "sale_amt") may lead to incorrect schema mapping.
   - Solution: Users are encouraged to use clear, descriptive column names.

2. Complex Queries:
   - Multi-step or highly nuanced questions (e.g., "Compare sales trends across regions after adjusting for inflation") may not translate perfectly.
   - Solution: Break complex queries into simpler, sequential questions.

3. Data Size:
   - Large datasets (e.g., >1GB) may slow down processing or hit memory limits for CSV uploads.
   - Solution: Optimize datasets by filtering or sampling before uploading, or rely on database connections for large-scale data.

4. Limited Context:
   - KOWALSKI relies on the connected database or uploaded CSV, lacking external context beyond TwelveData API stock data.
   - Solution: Provide relevant data within the database or CSV for comprehensive analysis.

5. Forecasting Accuracy:
   - Predictions are based on historical data patterns and TwelveData API inputs, which may not account for all external factors (e.g., market disruptions).
   - Solution: Use forecasts as guides, not definitive outcomes, and validate with domain expertise.

6. Language Ambiguity:
   - Vague queries (e.g., "Show me performance") may produce irrelevant results.
   - Solution: Use specific, well-phrased questions for accurate SQL generation.

7. Database Compatibility:
   - While KOWALSKI supports major database systems, some proprietary formats may require additional configuration.
   - Solution: Ensure databases use standard SQL-compatible structures.

## Technical Architecture

KOWALSKI's backend and frontend are designed for modularity, performance, and scalability, ensuring it can handle the needs of both small businesses and large corporations:

### Backend
- NLP Engine: Leverages the Grok 4 API, fine-tuned for SQL generation, directly addressing the hackathon's problem statement.
- Schema Parser: Custom algorithm to infer data types and relationships from company databases or CSV files.
- SQL Executor: Lightweight in-memory database for CSV processing or direct database connections for real-time queries.
- Forecasting Module: Time-series analysis integrated with the TwelveData API for real-time stock data predictions.
- Tech Stack: Python, JavaScript.

### Frontend
- Interface: Next.js-based single-page application for database connections, CSV uploads, and query input.
- Visualizations: Generates dynamic charts and graphs for intuitive data exploration.
- Tech Stack: Next.js, JavaScript.

### Workflow Example
1. User connects to their company database or uploads a CSV via the Next.js frontend.
2. Backend parses the database or CSV and builds a schema.
3. User inputs a query, processed by the Grok 4 API-powered NLP model to generate SQL.
4. SQL is executed on the connected database or in-memory dataset.
5. Results are visualized and displayed with textual insights, enhanced by real-time stock data from the TwelveData API.

## Why KOWALSKI Stands Out

KOWALSKI is a game-changer for FutureHack and beyond because it:
- Democratizes Data Analysis: No SQL or coding skills required, anyone can use it, aligning perfectly with the problem statement.
- Scales Seamlessly: Designed for small businesses and large corporations, with direct database access for enterprise-grade usability.
- Saves Time: Instant insights via database connections or CSV uploads, eliminating repetitive manual processes.
- Drives Decisions: Predictive analytics with TwelveData API integration empowers strategic planning, a significant innovation over the basic NLP-to-SQL requirement.
- Hackathon-Ready: Lightweight yet extravagant design, tailored for FutureHack's fast-paced environment, with versatility for real-world applications.

## Example Usage

1. Connect to Database or Upload a CSV:
   - Sample database table or CSV (`sales_data.csv`):
     ```csv
     date,product,region,sales,returns
     2025-01-01,Widget A,North,1000,50
     2025-01-01,Widget B,South,800,20
     ...
     ```

2. Ask Questions:
   - Query: "What are total sales by region?"
   - Generated SQL:
     ```sql
     SELECT region, SUM(sales) AS total_sales
     FROM sales_data
     GROUP BY region;
     ```
   - Output: Bar chart showing sales by region with a summary like "North region leads with $10,000 in sales."

3. Forecasting:
   - Query: "What's the sales forecast for Widget A?"
   - Output: Line chart predicting sales trends using TwelveData API data, with a recommendation like "Increase Widget A inventory by 10% for Q4."

## Future Enhancements

To make KOWALSKI even more powerful, we plan to:
- Expand database compatibility for broader enterprise adoption.
- Enhance NLP for complex, multi-step queries using advanced Grok 4 API capabilities.
- Optimize for large datasets with distributed processing.
- Integrate additional external data sources for richer context.
- Add multi-language support for global accessibility.

## Acknowledgments

- Built for FutureHack with love by PythonInMyBackend.
- Powered by open-source libraries, the Grok 4 API, and the TwelveData API.