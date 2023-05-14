import elasticsearch
from elasticsearch import Elasticsearch, helpers, RequestError
from pyspark.sql import SparkSession
import os
import datetime
import pandas as pd

spark = SparkSession.builder.appName("ParquetToES").getOrCreate()
parquet_dir = "C:\\Users\\Kimo Store\\Desktop\\weather-stations-monitoring\\centralStation\\archive"
timeStamp = 0

# for file in os.listdir(parquet_dir):
for (dirpath, _, filenames) in os.walk(parquet_dir):
    for file in filenames:
        if file.endswith("crc"):
            continue
        filename = os.path.basename(file)[:-len(".parquet")]
        file_timestamp = int(filename)
        if file.endswith(".parquet"):
            df = pd.read_parquet(os.path.join(dirpath, file))
            #
            #
            #
            index_name = "weather-stations4"

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
