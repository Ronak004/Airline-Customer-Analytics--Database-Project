# Airline Customer Analytics (SQL + MongoDB)

## Project Overview
This project performs customer and operational analytics for airlines using MongoDB aggregation pipelines and SQL queries. The workflows focus on extracting insights from airline reviews, flight delay records, customer booking behavior, customer support data, and airline stock time series.

## Tech Stack
- SQL (analytics + feature engineering)
- MongoDB Aggregation Pipeline (operational + review analytics)
- JavaScript (MongoDB query scripts)

## SQL Queries Used
Implemented end-to-end analytics queries demonstrating strong SQL capability, including:
- Multi-table joins to combine customer, flight, and review datasets
- Aggregations using `GROUP BY`, `COUNT()`, `AVG()`, `SUM()` to compute KPI summaries
- Conditional metrics using `CASE WHEN` (e.g., delay/cancellation flags, recommendation splits)
- Time-based feature engineering using date functions (month/year bucketing)
- Ranking + top-N analysis using window functions (e.g., busiest or most delayed routes)
- Sorting and filtering with `ORDER BY`, `HAVING`, and nested subqueries for clean reporting outputs
