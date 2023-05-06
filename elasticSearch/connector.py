import elasticsearch
from elasticsearch import Elasticsearch, helpers, RequestError
from pyspark.sql import SparkSession
import os
import datetime

# Create a SparkSession
spark = SparkSession.builder.appName("ParquetToES").getOrCreate()

timeStamp = 0
parquet_dir = "parquet_files"
parquet_files = [os.path.join(parquet_dir, file)
                 for file in os.listdir(parquet_dir)
                 if (file.endswith(".parquet") & os.path.basename(file) > timeStamp)]

df = spark.read.parquet(*parquet_files)

pdf = df.toPandas()

# Define the Elasticsearch index name and type name
index_name = "weather-stations3"

# Define the Elasticsearch connection settings
es = Elasticsearch(["http://localhost:9200"])

for i, row in pdf.iterrows():
    humidity = row["weather"]["humidity"]
    temperature = row["weather"]["temperature"]
    wind_speed = row["weather"]["windSpeed"]
    doc = {
        "station_id": row["stationId"],
        "s_no": row["sNo"],
        "battery_status": row["batteryStatus"],
        "status_timestamp": row["statusTimestamp"],
        "weather": [{
            "humidity": humidity,
            "temperature": temperature,
            "wind_speed": wind_speed
        }
        ]
    }
    es.index(index=index_name, document=doc)
