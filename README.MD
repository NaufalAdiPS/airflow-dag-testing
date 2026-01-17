Airflow DAG Testing Assignment

This repository contains a simple Apache Airflow project created for a DAG testing assignment.
The goal of this project is to demonstrate how to build, test, and validate an ETL pipeline using Airflow and pytest.

Project Overview

The DAG simulates a basic ETL process:

Extract: Generate sample data

Transform: Convert the name field to uppercase

Load: Simulate loading the transformed data

Testing is performed using:

airflow tasks test

pytest

Project Structure
airflow/
├── dags/
│   └── dag_testing_assignment.py
├── tests/
│   └── test_dag_testing_assignment.py
├── Screenshoot/
│   ├── extract.png
│   ├── transform.png
│   ├── load.png
│   └── pytest_test.png
├── docker-compose.yaml
├── .gitignore
└── README.md

How to Run the Project
1. Start Airflow
docker-compose up -d

2. Create Admin User
docker-compose run --rm airflow airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin


Access Airflow UI:

http://localhost:8080


Login:

Username: admin

Password: admin

DAG Testing

Run tasks individually using Airflow CLI:

airflow tasks test data_validation_dag extract_task 2025-10-22
airflow tasks test data_validation_dag transform_task 2025-10-22
airflow tasks test data_validation_dag load_task 2025-10-22


Each task supports isolated execution using fallback data.

Unit Testing with Pytest

Run unit tests with:

pytest tests/test_dag_testing_assignment.py


Tests include:

Transformation logic validation

Empty input handling

DAG integrity check using DagBag

Screenshots

Execution results are documented in the Screenshoot/ folder:

Airflow task test results

Pytest execution output

Author

Naufal
Airflow DAG Testing Assignment
