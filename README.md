# Australian Rental Data Pipeline

## Project Overview
This project builds an end-to-end batch data pipeline to analyse rental prices across Australia.

The goal is to understand how rental prices vary based on:

- Location (state, suburb)
- Property type
- Property features (bedrooms, bathrooms, parking)

## Dataset
Australian rental listings dataset (~6700 records).

## Tech Stack
- PySpark (data cleaning)
- BigQuery (data warehouse)
- dbt (analytics modelling)
- Kestra (orchestration)
- Docker (reproducible environment)
- Looker Studio (dashboard)

## Architecture (planned)
Raw CSV → Spark Cleaning → BigQuery → dbt → Dashboard

## Status
Project setup phase.