import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, when, lit, expr
import logging
import sys

# Initialize Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def main(env, mongodb_db, transformed_collection, route_insights_collection, origin_insights_collection):
    try:

        # MongoDB URI
        mongo_uri = f"mongodb+srv://shashank_test:GrowDataSkills219@mongo-db-cluster.0iwho.mongodb.net/{mongodb_db}?directConnection=true"

        # Initialize SparkSession
        spark = SparkSession.builder \
            .appName("FlightBookingAnalysis") \
            .config("spark.mongodb.input.uri", mongo_uri) \
            .config("spark.mongodb.output.uri", mongo_uri) \
            .getOrCreate()

        logger.info("Spark session initialized.")

        # Resolve GCS path based on the environment
        input_path = f"gs://airflow-projetcs-gds/airflow-project-1/source-{env}"
        logger.info(f"Input path resolved: {input_path}")

        # Read the data from GCS
        data = spark.read.csv(input_path, header=True, inferSchema=True)
        logger.info("Data read from GCS.")

        # Data transformations
        logger.info("Starting data transformations.")

        # Add derived columns
        transformed_data = data.withColumn(
            "is_weekend", when(col("flight_day").isin("Sat", "Sun"), lit(1)).otherwise(lit(0))
        ).withColumn(
            "lead_time_category", when(col("purchase_lead") < 7, lit("Last-Minute"))
                                  .when((col("purchase_lead") >= 7) & (col("purchase_lead") < 30), lit("Short-Term"))
                                  .otherwise(lit("Long-Term"))
        ).withColumn(
            "booking_success_rate", expr("booking_complete / num_passengers")
        )

        # Aggregations for insights
        route_insights = transformed_data.groupBy("route").agg(
            count("*").alias("total_bookings"),
            avg("flight_duration").alias("avg_flight_duration"),
            avg("length_of_stay").alias("avg_stay_length")
        )

        booking_origin_insights = transformed_data.groupBy("booking_origin").agg(
            count("*").alias("total_bookings"),
            avg("booking_success_rate").alias("success_rate"),
            avg("purchase_lead").alias("avg_purchase_lead")
        )

        logger.info("Data transformations completed.")

        # Write transformed data to MongoDB
        logger.info(f"Writing transformed data to MongoDB collection: {transformed_collection}")
        transformed_data.write \
            .format("mongodb") \
            .option("spark.mongodb.connection.uri", mongo_uri) \
            .option("spark.mongodb.database", mongodb_db) \
            .option("spark.mongodb.collection", transformed_collection) \
            .mode("overwrite") \
            .save()

        # Write route insights to MongoDB
        logger.info(f"Writing route insights to MongoDB collection: {route_insights_collection}")
        route_insights.write \
            .format("mongodb") \
            .option("spark.mongodb.connection.uri", mongo_uri) \
            .option("spark.mongodb.database", mongodb_db) \
            .option("spark.mongodb.collection", route_insights_collection) \
            .mode("overwrite") \
            .save()

        # Write booking origin insights to MongoDB
        logger.info(f"Writing booking origin insights to MongoDB collection: {origin_insights_collection}")
        booking_origin_insights.write \
            .format("mongodb") \
            .option("spark.mongodb.connection.uri", mongo_uri) \
            .option("spark.mongodb.database", mongodb_db) \
            .option("spark.mongodb.collection", origin_insights_collection) \
            .mode("overwrite") \
            .save()

        logger.info("Data written to MongoDB successfully.")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
        sys.exit(1)

    finally:
        # Stop Spark session
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Process flight booking data and write to MongoDB.")
    parser.add_argument("--env", required=True, help="Environment (e.g., dev, prod)")
    parser.add_argument("--mongodb_db", required=True, help="MongoDB database name")
    parser.add_argument("--transformed_collection", required=True, help="MongoDB collection for transformed data")
    parser.add_argument("--route_insights_collection", required=True, help="MongoDB collection for route insights")
    parser.add_argument("--origin_insights_collection", required=True, help="MongoDB collection for booking origin insights")

    args = parser.parse_args()

    # Call the main function with parsed arguments
    main(
        env=args.env,
        mongodb_db=args.mongodb_db,
        transformed_collection=args.transformed_collection,
        route_insights_collection=args.route_insights_collection,
        origin_insights_collection=args.origin_insights_collection
    )