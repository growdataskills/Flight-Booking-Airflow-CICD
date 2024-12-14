from datetime import datetime, timedelta
import uuid  # Import UUID for unique batch IDs
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.python import PythonOperator
import time

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
    dag_id="flight_booking_dataproc_bq_dag",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or on-demand
    catchup=False,
) as dag:

    # Fetch environment variables
    env = Variable.get("env", default_var="dev")
    gcs_bucket = Variable.get("gcs_bucket", default_var="airflow-projetcs-gds")
    bq_project = Variable.get("bq_project", default_var="psyched-service-442305-q1")
    bq_dataset = Variable.get("bq_dataset", default_var=f"flight_data_{env}")
    tables = Variable.get("tables", deserialize_json=True)

    # Extract table names from the 'tables' variable
    transformed_table = tables["transformed_table"]
    route_insights_table = tables["route_insights_table"]
    origin_insights_table = tables["origin_insights_table"]

    # Generate a unique batch ID using UUID
    batch_id = f"flight-booking-batch-{str(uuid.uuid4())[:8]}"  # Shortened UUID for brevity

    # Custom Python function to check for .csv files in the prefix
    def wait_for_csv_file(**kwargs):
        bucket = gcs_bucket
        prefix = f"airflow-project-1/source-{env}/"
        max_wait_time = 300  # 5 minutes
        interval = 30  # Check every 30 seconds

        start_time = time.time()
        gcs_hook = GCSHook()

        while time.time() - start_time < max_wait_time:
            files = gcs_hook.list(bucket_name=bucket, prefix=prefix)
            csv_files = [file for file in files if file.endswith(".csv")]

            if csv_files:
                # Push the found file to XCom (optional)
                kwargs['ti'].xcom_push(key='csv_file_name', value=csv_files[0])
                return f"File found: {csv_files[0]}"

            time.sleep(interval)

        raise FileNotFoundError("No .csv file found within the timeout period.")

    # Task 1: Custom PythonOperator to check for .csv files
    check_csv_files_task = PythonOperator(
        task_id="check_csv_files_in_gcs",
        python_callable=wait_for_csv_file,
        provide_context=True,
    )

    # # Task 1: File Sensor for GCS
    # file_sensor = GCSObjectsWithPrefixExistenceSensor(
    #     task_id="check_file_existence",
    #     bucket=gcs_bucket,  # GCS bucket
    #     prefix=f"airflow-project-1/source-{env}/",  # GCS path
    #     google_cloud_conn_id="google_cloud_default",  # GCP connection
    #     timeout=300,  # Timeout in seconds
    #     poke_interval=30,  # Time between checks
    #     mode="poke",  # Blocking mode
    # )

    # Task 2: Submit PySpark job to Dataproc Serverless
    batch_details = {
        "pyspark_batch": {
            "main_python_file_uri": f"gs://{gcs_bucket}/airflow-project-1/spark-job/spark_transformation_job.py",  # Main Python file
            "python_file_uris": [],  # Python WHL files
            "jar_file_uris": ["gs://airflow-projetcs-gds/airflow-project-1/spark-jars/mongo-spark-connector_2.12-10.3.0-all.jar"],  # JAR files
            "args": [
                f"--env={env}",
                f"--bq_project={bq_project}",
                f"--bq_dataset={bq_dataset}",
                f"--transformed_table={transformed_table}",
                f"--route_insights_table={route_insights_table}",
                f"--origin_insights_table={origin_insights_table}",
            ]
        },
        "runtime_config": {
            "version": "2.2",  # Specify Dataproc version (if needed)
        },
        "environment_config": {
            "execution_config": {
                "service_account": "70622048644-compute@developer.gserviceaccount.com",
                "network_uri": "projects/psyched-service-442305-q1/global/networks/default",
                "subnetwork_uri": "projects/psyched-service-442305-q1/regions/us-central1/subnetworks/default",
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
    # file_sensor >> pyspark_task

    check_csv_files_task >> pyspark_task