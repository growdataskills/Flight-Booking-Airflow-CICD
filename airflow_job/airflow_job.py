from datetime import datetime, timedelta
import uuid  # Import UUID for unique batch IDs
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
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
    dag_id="flight_booking_dataproc_serverless_dag",
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

    # Generate a unique batch ID using UUID
    batch_id = f"flight-booking-batch-{str(uuid.uuid4())[:8]}"  # Shortened UUID for brevity

    # Task 1: File Sensor for GCS
    file_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id="check_file_existence",
        bucket="airflow-projetcs-gds",  # GCS bucket
        prefix=f"airflow-project-1/source-{env}/",  # GCS path
        google_cloud_conn_id="google_cloud_default",  # GCP connection
        timeout=300,  # Timeout in seconds
        poke_interval=30,  # Time between checks
        mode="poke",  # Blocking mode
    )

    # Task 2: Submit PySpark job to Dataproc Serverless
    batch_details={
        "pyspark_batch": {
            "main_python_file_uri": "gs://airflow-projetcs-gds/airflow-project-1/spark-job/spark_transformation_job.py",  # Main Python file
            "python_file_uris": [],  # Python WHL files
            "jar_file_uris": ["gs://airflow-projetcs-gds/airflow-project-1/spark-jars/mongo-spark-connector_2.12-10.3.0-all.jar"],  # JAR files
            "args": [
                f"--env={env}",
                f"--mongodb_db={mongodb_db}",
                f"--transformed_collection={transformed_collection}",
                f"--route_insights_collection={route_insights_collection}",
                f"--origin_insights_collection={origin_insights_collection}",
            ]
        },
        "runtime_config": {
            "version": "2.2",  # Specify Dataproc version (if needed)
        },
        "environment_config": {
            "execution_config": {
                "service_account": "70622048644-compute@developer.gserviceaccount.com",
                "subnetwork_uri": "projects/psyched-service-442305-q1/regions/us-central1/subnetworks/default",
                # Use public IPs for Dataproc Serverless instances
                "enable_network_egress_control": False,  # Allow external internet access
                "service_account": "70622048644-compute@developer.gserviceaccount.com",
                "network_tags": ["dataproc-serverless"],  # Optional: Add tags for egress firewall rules
            }
        },
    }

    pyspark_task = DataprocCreateBatchOperator(
                    task_id="run_spark_job_on_dataproc_serverless",
                    batch=batch_details,
                    batch_id=batch_id,
                    project_id="psyched-service-442305-q1",
                    region="us-central1",
                    gcp_conn_id="google_cloud_default",
                )

    # Task Dependencies
    file_sensor >> pyspark_task