Pricing Analysis Project
Overview
Situation
Businesses face challenges understanding how external factors like inflation and input costs impact profitability. With large datasets available from platforms like Snowflake Marketplace, it's possible to derive actionable insights to address these challenges.

Task
The goal of this project was to build a data pipeline that processes financial and economic data to answer key business questions, such as:

How might profitability be impacted based on input costs?
How have residential real estate property prices changed over time?
What trends exist in inflation and their effects on product pricing?
Pipeline Workflow
Action
Tools and Setup:

I used dbt for transforming data, Snowflake as the data warehouse, and Airflow for scheduling workflows.
Data was sourced from Snowflake Marketplace, focusing on datasets like bureau_of_labor_statistics_price_timeseries and related attributes.
Steps Taken:

Staging: Cleaned and joined raw data to create structured tables like stg_profitability_timeseries.
Intermediate Models: Processed data further to calculate trends, such as int_cost_trends and int_inflation_analysis.
Fact Tables: Built summary tables like fct_cost_trends and fct_inflation_analysis for easy reporting and analysis.
Visualization: Queried and visualized the results directly in Snowflake, providing insights into cost trends and inflation patterns.

Below are some key findings from the project, along with screenshots of the tables created:
![image](https://github.com/user-attachments/assets/5240b6c9-0ea4-4e04-9eb1-b2b081816159)
![image](https://github.com/user-attachments/assets/d5aa18f2-0fc7-4517-94dc-502c329bb726)




What I Learned
SQL Optimization: Writing efficient queries to clean and transform data.
Data Modeling with dbt: Creating reusable and well-documented models.
Data Analysis: Extracting meaningful insights from structured data.
Pipeline Automation: Setting up workflows using Airflow.
