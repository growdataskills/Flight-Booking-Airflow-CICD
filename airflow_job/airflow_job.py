from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.models import Variable

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 14),
}

# Define the DAG
with DAG(
    dag_id="flight_booking_dataproc_dag",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or on-demand
    catchup=False,
) as dag:

    # Environment variable (read from Airflow Variables)
    env = Variable.get("env", default_var="dev")
    mongodb_db = Variable.get("mongodb_db", default_var="flight_data")
    transformed_collection = Variable.get("transformed_collection", default_var="transformed_data")
    route_insights_collection = Variable.get("route_insights_collection", default_var="route_insights")
    origin_insights_collection = Variable.get("origin_insights_collection", default_var="booking_origin_insights")
    gcs_path = f"gs://airflow-projects-gds/airflow-project-1/source-{env}"

    # Task 1: File Sensor for GCS
    file_sensor = GCSObjectExistenceSensor(
        task_id="check_file_existence",
        bucket="airflow-projects-gds",  # GCS bucket
        object=f"airflow-project-1/source-{env}",  # GCS path
        google_cloud_conn_id="google_cloud_default",  # GCP connection
        timeout=300,  # Timeout in seconds
        poke_interval=30,  # Time between checks
        mode="poke",  # Blocking mode
    )

    # Task 2: Submit PySpark job to Dataproc
    dataproc_job_config = {
        "reference": {"project_id": "your-gcp-project-id"},
        "placement": {"cluster_name": "your-dataproc-cluster-name"},
        "pyspark_job": {
            "main_python_file_uri": "gs://path-to-your-pyspark-script/flight_booking_analysis.py",
            "args": [
                f"--env={env}",
                f"--mongodb_db={mongodb_db}",
                f"--transformed_collection={transformed_collection}",
                f"--route_insights_collection={route_insights_collection}",
                f"--origin_insights_collection={origin_insights_collection}",
            ],
        },
    }

    pyspark_task = DataprocSubmitJobOperator(
        task_id="run_pyspark_job",
        job=dataproc_job_config,
        region="your-dataproc-region",
        project_id="your-gcp-project-id",
        gcp_conn_id="google_cloud_default",
    )

    # Task Dependencies
    file_sensor >> pyspark_task