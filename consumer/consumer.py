from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import from_json, col
from config import postgres_config, checkpoint_dir, kafka_data_schema
from pyspark.sql import DataFrame

# Initialize Spark session
spark = (SparkSession.builder
         .appName('KafkaSparkStreaming')
         .getOrCreate() 
)

# Read stream from Kafka
df = (spark.readStream.format('kafka')
      .option('kafka.bootstrap.servers', 'kafka:9092')
      .option('subscribe', 'stock_analysis')  # Topic subscription
      .option('startingOffsets', 'latest')  # Read only new incoming messages
      .option('failOnDataLoss', 'false')  # Continue reading even if Kafka deletes old messages
      .load()  # Start reading the Kafka topic as a stream
)

# Convert the 'value' column (which is a JSON string) into structured columns
parsed_df = (df.selectExpr('CAST(value AS STRING)')
             .select(from_json(col("value"), kafka_data_schema).alias("data"))
             .select("data.*")
)

# Process the data to format it appropriately
processed_df = parsed_df.select(
    col("date").cast(TimestampType()).alias("date"),
    col("high").alias("high"),
    col("low").alias("low"),
    col("open").alias("open"),
    col("close").alias("close"),
    col("symbol").alias("symbol")
)

def write_to_postgres(batch_df: DataFrame, batch_id: int) -> None:
    """
    Writes a microbatch DataFrame to PostgreSQL using JDBC in 'append' mode.

    Args:
        batch_df (DataFrame): The DataFrame to be written to PostgreSQL.
        batch_id (int): The unique ID for the microbatch. Used for tracking purposes.

    This function writes the processed DataFrame to PostgreSQL in the 'append' mode.
    It ensures that the data from Kafka is efficiently written to the target database.
    """
    
    batch_df.write \
        .format("jdbc") \
        .mode("append") \
        .options(**postgres_config) \
        .save()

# Stream the data to PostgreSQL using foreachBatch
query = (processed_df.writeStream
         .foreachBatch(write_to_postgres)
         .option('checkpointLocation', checkpoint_dir)  # Checkpoint directory for fault tolerance
         .outputMode('append')
         .start()
)

# Wait for the termination of the query
query.awaitTermination()

