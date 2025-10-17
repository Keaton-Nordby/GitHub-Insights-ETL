# DataFlow — Python ETL Data Pipeline MVP

Overview

This repository contains a minimal, production-style Python ETL pipeline that fetches GitHub repository metadata, transforms it, stores it into an SQLite database using SQLAlchemy, and generates daily summary reports.

Project structure

/dataflow_project
├── src/ (core modules)
├── config/ (config.yaml, .env.example)
├── reports/ (generated reports)
├── tests/ (pytest unit tests)
├── requirements.txt
├── Makefile
└── README.md

Quick start

1. Copy `.env.example` to `.env` and set `GITHUB_TOKEN` (optional for public data).
2. Install dependencies:

   pip install -r requirements.txt

3. Run the pipeline:

   python src/main.py

4. Check `reports/` for `daily_summary.csv` and `daily_summary.json`.

Architecture

- Fetcher: retrieves repository data from GitHub.
- Transformer: converts raw JSON into pandas DataFrames and computes derived metrics.
- Loader: writes repositories and metrics into SQLite via SQLAlchemy.
- Reporter: generates CSV/JSON summary reports.
- Logger: centralized structured logging.

Testing

Run tests with `pytest`.

Notes

This is an MVP demonstration. For production use, add pagination limits, caching, rate-limit handling, and more robust upserts.
