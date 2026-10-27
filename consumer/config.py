from pyspark.sql.types import StructType, StructField, StringType
import os

# directory where Spark will store its checkpoint data. crucial in streaming to enable fault tolerance
checkpoint_dir = "/tmp/checkpoint/kafka_to_postgres" 
if not os.path.exists(checkpoint_dir):
  os.makedirs(checkpoint_dir)

postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')
postgres_config = {
    "url": "jdbc:postgresql://postgres:5432/stock_data",
    "user": postgres_user,       
    "password": postgres_password, 
    "dbtable": "stocks", 
    "driver": "org.postgresql.Driver"
}

# The schema/structure matching the new data coming from Kafka
kafka_data_schema = StructType([
  StructField("date", StringType()), 
  StructField("high", StringType()),
  StructField("low", StringType()),
  StructField("open", StringType()),
  StructField("close", StringType()),
  StructField("symbol", StringType())
])
