Airflow DAG Testing Assignment
📌 Project Description

This project is a simple ETL pipeline simulation using Apache Airflow, designed to demonstrate DAG testing and unit testing using:

airflow tasks test

pytest

The pipeline consists of three main stages:

Extract – retrieves dummy data

Transform – converts the name field to uppercase

Load – simulates loading transformed data

The project follows best practices for Airflow DAG structure, testing, and documentation.

🗂️ Project Structure
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

⚙️ Prerequisites

Make sure you have installed:

Docker

Docker Compose

Git

🚀 Running Airflow
1️⃣ Start Airflow
docker-compose up -d

2️⃣ Create Airflow Admin User

Run the following command to create an admin user:

docker-compose run --rm airflow airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin


Access the Airflow UI at:

http://localhost:8080


Login credentials:

Username: admin

Password: admin

🧪 DAG Testing Using Airflow CLI
🔹 Extract Task
airflow tasks test data_validation_dag extract_task 2025-10-22

🔹 Transform Task
airflow tasks test data_validation_dag transform_task 2025-10-22

🔹 Load Task
airflow tasks test data_validation_dag load_task 2025-10-22


The airflow tasks test command runs tasks in isolated mode.
Fallback data is implemented inside tasks to support isolated execution.

✅ Unit Testing with Pytest

Run unit tests using:

pytest tests/test_dag_testing_assignment.py


The tests include:

Unit testing for the transform_data function

Empty input handling

DAG integrity testing using DagBag

📸 Screenshots

Execution screenshots for:

Airflow task tests (extract, transform, load)

Pytest test results

Available in the:

Screenshoot/


directory.

🧠 Technical Notes

Uses XCom for inter-task communication

Implements fallback logic for isolated task execution

Follows Airflow production-ready structure

👤 Author

Name: Naufal
Role: Data Engineering / Airflow Assignment
