from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 12, 14),
}

# Python function to print variables
def print_variables():
    env = Variable.get("env", default_var="dev")
    mongodb_db = Variable.get("mongodb_db", default_var="flight_data")
    transformed_collection = Variable.get("transformed_collection", default_var="transformed_data")
    route_insights_collection = Variable.get("route_insights_collection", default_var="route_insights")
    origin_insights_collection = Variable.get("origin_insights_collection", default_var="booking_origin_insights")

    print(f"Environment: {env}")
    print(f"MongoDB Database: {mongodb_db}")
    print(f"Transformed Collection: {transformed_collection}")
    print(f"Route Insights Collection: {route_insights_collection}")
    print(f"Origin Insights Collection: {origin_insights_collection}")

# Define the DAG
with DAG(
    dag_id="test_airflow_variables",
    default_args=default_args,
    schedule_interval=None,  # Trigger manually or on-demand
    catchup=False,
) as dag:

    # Task: Print Variables
    print_vars_task = PythonOperator(
        task_id="print_variables",
        python_callable=print_variables,
    )

    # Task dependencies (if required, but here it's standalone)
    print_vars_task