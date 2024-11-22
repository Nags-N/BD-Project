from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, from_json, to_json, struct, count, unix_timestamp, expr
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
import os
import requests
import json

class EmojiStreamProcessor:
    def __init__(self):
        # Initialize Spark session
        self.spark = SparkSession.builder \
            .appName("EmojiStreamProcessor") \
            .master("local[*]") \
            .getOrCreate()

        # Load configuration
        self.input_topic = os.getenv("KAFKA_INPUT_TOPIC", "emoji-input")
        self.output_topic = os.getenv("KAFKA_OUTPUT_TOPIC", "emoji-main")
        self.kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")
        self.checkpoint_location = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoint")
        self.client_registration_url = os.getenv("CLIENT_REGISTRATION_URL", "http://localhost:5001/clients/")  # Registration URL

        # Schema for incoming data
        self.schema = StructType([
            StructField("user_id", IntegerType(), True),  # Correct use of StructField
            StructField("emoji_type", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("subscriber_id", StringType(), True)  # Assuming this field exists
        ])

    def create_input_stream(self):
        """Creates a structured streaming input from Kafka."""
        return self.spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", self.kafka_broker) \
            .option("subscribe", self.input_topic) \
            .option("failOnDataLoss", "false") \
            .load()

    def get_registered_clients(self, subscriber_id):
        """Fetch the registered clients for a given subscriber."""
        try:
            response = requests.get(f'{self.client_registration_url}{subscriber_id}')
            if response.status_code == 200:
                return response.json().get('clients', [])
            else:
                print(f"Failed to fetch clients for {subscriber_id}: {response.status_code}")
                return []
        except requests.exceptions.RequestException as e:
            print(f"Error fetching registered clients for {subscriber_id}: {e}")
            return []

    def process_stream(self):
        """Processes the Kafka input stream."""
        # Read from Kafka topic
        raw_stream = self.create_input_stream()

        # Parse Kafka messages
        emoji_data = raw_stream.selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), self.schema).alias("data")) \
            .select("data.*")

        # Add timestamp for windowing
        emoji_data = emoji_data.withColumn("event_time", col("timestamp").cast(TimestampType()))

        # Window-based aggregation
        aggregated_data = emoji_data \
            .withWatermark("event_time", "10 seconds") \
            .groupBy(
                window(col("event_time"), "5 seconds", "2 seconds"),
                col("emoji_type"),
                col("subscriber_id")  # Include subscriber_id for filtering
            ).agg(count("user_id").alias("count")) \
            .select(
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("emoji_type"),
                col("count"),
                col("subscriber_id")  # Include subscriber_id in the result
            )

        # Prepare output for Kafka
        output_data = aggregated_data.select(
            to_json(struct(
                col("window_start"),
                col("window_end"),
                col("emoji_type"),
                col("count"),
                col("subscriber_id")
            )).alias("value")
        )

        # Write aggregated data to Kafka
        def forward_to_clients(aggregated_data):
            """Forwards aggregated data to the registered clients."""
            subscriber_id = aggregated_data['subscriber_id']
            registered_clients = self.get_registered_clients(subscriber_id)
            
            if registered_clients:
                for client_id in registered_clients:
                    payload = {
                        'subscriber_id': subscriber_id,
                        'client_id': client_id,
                        'data': aggregated_data
                    }
                    # Send the data to Kafka topic for registered clients
                    self.producer.send(self.output_topic, json.dumps(payload).encode('utf-8'))
                    print(f"Forwarded aggregated data to client {client_id}: {payload}")
            else:
                print(f"No registered clients found for subscriber {subscriber_id}, skipping data forwarding.")

        # Perform the forwarding inside an output operation
        output_data.writeStream \
            .foreachBatch(lambda df, epoch_id: df.collect().foreach(forward_to_clients)) \
            .option("checkpointLocation", f"{self.checkpoint_location}/kafka") \
            .outputMode("update") \
            .trigger(processingTime="5 seconds") \
            .start()

        return aggregated_data

    def run(self):
        """Runs the streaming processor."""
        try:
            self.process_stream()
            print("Streaming processor is running...")
            self.spark.streams.awaitAnyTermination()
        except Exception as e:
            print(f"Error in stream processing: {e}")
        finally:
            print("Shutting down Spark...")
            self.spark.stop()


if __name__ == "__main__":
    processor = EmojiStreamProcessor()
    processor.run()
