import elasticsearch
from elasticsearch import Elasticsearch, helpers, RequestError
from pyspark.sql import SparkSession
import os
import datetime
import pandas as pd

spark = SparkSession.builder.appName("ParquetToES").getOrCreate()
parquet_dir = "parquet_files"
timeStamp = 0

for file in os.listdir(parquet_dir):
    filename = datetime.datetime.strptime(os.path.basename(file)[:-len(".parquet")], '%Y-%m-%dT%H-%M-%S.%f')
    file_timestamp = filename.timestamp()
    if file.endswith(".parquet") and file_timestamp > timeStamp:
        df = pd.read_parquet(os.path.join(parquet_dir, file))
        #
        #
        #
        index_name = "weather-stations3"

        # Define the Elasticsearch connection settings
        es = Elasticsearch(["http://localhost:9200"])

        for i, row in df.iterrows():
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
            #
            #
            #

        timeStamp = file_timestamp  # update timeStamp
        print("Loaded file with timestamp", timeStamp)

print("Latest timestamp:", timeStamp)
