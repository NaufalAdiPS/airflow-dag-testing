Run this for user creation

docker-compose run --rm airflow airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin


# airflow@xxxxxxxx:/opt/airflow$
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin

# Extract
airflow tasks test data_validation_dag extract_task 2025-10-22

# Transform
airflow tasks test data_validation_dag transform_task 2025-10-22

# Load
airflow tasks test data_validation_dag load_task 2025-10-22

# Untuk Test pytest
pytest tests/test_dag_testing_assignment.py


docker-compose run --rm airflow airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
