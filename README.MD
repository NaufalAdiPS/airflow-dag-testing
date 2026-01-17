# Airflow DAG Testing Assignment

Airflow DAG Testing Assignment is a simple ETL pipeline using Apache Airflow to demonstrate DAG development, task testing, and unit testing with pytest.

## Installation 

Use Docker and Docker Compose to run this project.

```bash
git clone <your-repository-url>
cd airflow
docker-compose up -d
```

## Create an Airflow admin user

``` bash
docker-compose run --rm airflow airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin
```



## Testing

Test Airflow tasks individually using airflow tasks test:
```bash
# Extract task
airflow tasks test data_validation_dag extract_task 2025-10-22

# Transform task
airflow tasks test data_validation_dag transform_task 2025-10-22

# Load task
airflow tasks test data_validation_dag load_task 2025-10-22

```
Run unit tests using pytest:

```bash
pytest tests/test_dag_testing_assignment.py


```

## Author
Naufal Adi P.S
