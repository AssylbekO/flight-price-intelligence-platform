-- Create a separate database dedicated entirely to Airflow metadata
CREATE DATABASE airflow_metadata;

-- Connect to your primary analytics data warehouse database
\c flight_intelligence;

-- Create the distinct architectural schemas for your Medallion layers
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS mart;