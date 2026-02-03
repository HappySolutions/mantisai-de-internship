## Week 1 - ETL Pipeline

This project implements a simple CSV-based ETL (Extract, Transform, Load) pipeline using Python.  
It is built as part of a Data Engineering training program to practice clean code, defensive programming, logging, testing, and job scheduling.

---

## Project Description

The goal of this project is to demonstrate how a basic ETL pipeline can be designed and implemented using Python best practices.  
The pipeline extracts data from a CSV file, cleans and validates the data, and then loads the cleaned output into a new CSV file.  
The project emphasizes modular, testable, and maintainable code suitable for real-world data engineering workflows.

---

## Features

- Extract data from CSV files
- Transform and clean data (null handling, type casting, validation)
- Load cleaned data into a new CSV file
- Modular and reusable transformation logic
- Logging to file and console
- Unit tests using pytest
- Automated execution using Windows Task Scheduler

---

### How to run

1. Create and activate a virtual environment:

```bash
python etl\pipeline.py
 python -m venv venv
   venv\Scripts\activate
pip install pandas pytest python-dotenv loguru
python etl\pipeline.py
```

### How to test

pytest
All tests should pass successfully.

### Scheduling

The ETL pipeline is scheduled to run daily using Windows Task Scheduler.

The task runs the Python interpreter with the path to pipeline.py

Execution status and errors are logged to the logs/ directory

This ensures the pipeline runs automatically without manual intervention

Notes

The transformation logic is isolated into reusable functions to simplify testing and maintenance.

Invalid records are safely ignored without stopping the pipeline execution.

This project focuses on correctness, readability, and robustness rather than performance optimization.
