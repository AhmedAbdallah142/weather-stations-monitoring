import time
import os

import pandas as pd
from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

spark = SparkSession.builder.appName("ParquetToES").getOrCreate()
parquet_dir = "../data/archive"
timeStamp = 0

index_name = "weather-station"

# Define the Elasticsearch connection settings
elastic_search = os.environ.get('elastic_search')
if elastic_search is None:
    elastic_search = "localhost"
es = Elasticsearch([f"{elastic_search}:9200"])

mappings = {
    "properties": {
        "battery_status": {
            "type": "keyword"
        },
        "s_no": {
            "type": "long"
        },
        "station_id": {
            "type": "long"
        },
        "status_timestamp": {
            "type": "date"
        },
        "weather": {
            "properties": {
                "humidity": {
                    "type": "integer"
                },
                "temperature": {
                    "type": "integer"
                },
                "wind_speed": {
                    "type": "integer"
                }
            }
        }
    }
}

if es.indices.exists(index=index_name):
    print(f"The index {index_name} exists.")
else:
    es.indices.create(index=index_name, mappings=mappings)


class MyHandler(FileSystemEventHandler):
    def on_created(self, event):
        print("File created:", event.src_path)
        if event.is_directory:
            return
        if event.src_path.endswith(".parquet") and not event.src_path.endswith(".crc"):
            # read the newly added file
            df = pd.read_parquet(event.src_path)
            # do something with the data in the file here

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


# create an observer to watch the directory for changes
observer = Observer()
observer.schedule(MyHandler(), path=parquet_dir, recursive=True)

# start the observer
observer.start()

while True:
    try:
        # sleep for a short time before checking for changes again
        time.sleep(1)
    except KeyboardInterrupt:
        # stop the observer if the user presses Ctrl-C
        observer.stop()
        break

print("Latest timestamp:", timeStamp)
