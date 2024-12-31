import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json, col, month, to_timestamp
from pyspark.sql.functions import expr


class KafkaConsumerToDelta:
    def __init__(
        self, topic: str, bootstrap_servers: str, delta_table_path: str
    ) -> None:
        """
        Initialize a Kafka Consumer.
        :param topic: Kafka topic to subscribe to
        :param group_id: Consumer group ID
        :param bootstrap_servers: Kafka broker addresses (e.g., "localhost:9092")
        :param delta_table_path: Delta table path for storing messages
        """
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.delta_table_path = delta_table_path

        logging.info(f"Kafka Consumer initialized for topic: {self.topic}")

    def consume_write_delta(self) -> None:
        spark = (
            SparkSession.builder.appName("ldi")
            .config("spark.executor.memory", "8g")
            .config("spark.driver.memory", "8g")
            .config("spark.executor.cores", "4")
            .config("spark.driver.cores", "4")
            .config(
                "spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,io.delta:delta-core_2.12:2.3.0",
            )
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .getOrCreate()
        )

        kafka_stream = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "input-topic")
            .option("startingOffsets", "earliest")
            .load()
        )

        json_schema = StructType(
            [
                StructField("username", StringType(), True),
                StructField("first_name", StringType(), True),
                StructField("last_name", StringType(), True),
                StructField("email", StringType(), True),
                StructField("date_created", StringType(), True),
            ]
        )

        # deserialize df
        json_stream = kafka_stream.selectExpr("CAST(value AS STRING)")

        # Apply Schema to JSON value column and expand the value
        parsed_stream = json_stream.withColumn(
            "parsed_value", from_json(col("value"), json_schema)
        )
        # Select the parsed fields into a new DataFrame
        parsed_stream = parsed_stream.select("parsed_value.*")

        parsed_stream = parsed_stream.withColumn(
            "timestamp",
            to_timestamp(
                expr("substring(date_created, 1, 23)"), "yyyy-MM-dd HH:mm:ss.SSS"
            ),
        )
        parsed_stream = parsed_stream.withColumn("initial", col("username"))
        final_stream = parsed_stream.withColumn("month", month(col("timestamp")))
        final_stream = final_stream.repartitionByRange("initial")

        # write in Delta table
        (
            final_stream.writeStream.format("delta")
            .outputMode("append")
            .option(
                "checkpointLocation",
                "/tmp/ldi_pyspark_streaming_testing/checkpoint",
            )
            .option("partitionBy", "initial")
            .start("/tmp/ldi_pyspark_streaming_testing")
            .awaitTermination()
        )


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.WARN)
    logger = logging.getLogger(__name__)

    # Define parameters
    topic = "input-topic"
    bootstrap_servers = "localhost:9092"  # Replace with your Kafka broker address
    delta_table_path = "/tmp/ldi_pyspark_streaming_testing/deltatables"

    # Create an instance of KafkaConsumerClient
    kafka_consumer = KafkaConsumerToDelta(topic, bootstrap_servers, delta_table_path)

    # Start consuming messages
    kafka_consumer.consume_write_delta()

    logger.info("All messages consumed and written to Delta table successfully.")
