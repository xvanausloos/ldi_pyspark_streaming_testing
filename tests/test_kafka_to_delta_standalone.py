import unittest
from unittest.mock import patch, MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import Row

class TestKafkaStreamingUnit(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("KafkaStreamingUnitTest")
            .master("local[2]")
            .getOrCreate()
        )

        cls.schema = StructType(
            [
                StructField("id", StringType(), True),
                StructField("value", StringType(), True),
            ]
        )

    def test_kafka_streaming_with_mocks(self):
        # Create sample data
        sample_data = [
            Row(key=b"1", value=b'{"id": "1", "value": "test1"}'),
            Row(key=b"2", value=b'{"id": "2", "value": "test2"}'),
        ]

        # Create DataFrame with sample data
        input_df = self.spark.createDataFrame(sample_data)

        # Mock the streaming read
        with patch("pyspark.sql.streaming.DataStreamReader") as mock_reader:
            mock_reader.load.return_value = input_df

            # Mock the streaming write
            with patch("pyspark.sql.streaming.DataStreamWriter") as mock_writer:
                mock_query = MagicMock()
                mock_writer.start.return_value = mock_query

                # Test your streaming logic
                def process_stream(df):
                    return df.selectExpr("CAST(value AS STRING) as json")

                # Process the stream
                result_df = process_stream(input_df)

                # Verify results
                actual_values = [row.json for row in result_df.collect()]
                expected_values = [
                    '{"id": "1", "value": "test1"}',
                    '{"id": "2", "value": "test2"}',
                ]
                self.assertEqual(actual_values, expected_values)

    @classmethod
    def tearDownClass(cls):
        if cls.spark:
            cls.spark.stop()


if __name__ == "__main__":
    unittest.main()
