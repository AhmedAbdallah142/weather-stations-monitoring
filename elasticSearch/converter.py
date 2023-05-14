from elasticsearch import Elasticsearch

es = Elasticsearch(["http://localhost:9200"])

# Define the Elasticsearch index name
index_name = "weather-stations4"

# Define the station ID to search for
station_id = 1

# Define the range of s_no values to check
start_s_no = 1
end_s_no = 1000

# Create the Elasticsearch query
query = {
    "query": {
        "bool": {
            "must": [
                {"term": {"station_id": station_id}},
                {"range": {"s_no": {"gte": start_s_no, "lte": end_s_no}}}
            ]
        }
    },
    "size": 1000
}

# Execute the query and get the s_no values
s_no_values = set()
result = es.search(index=index_name, body=query)
hits = result["hits"]["hits"]
for hit in hits:
    s_no = hit["_source"]["s_no"]
    s_no_values.add(s_no)

# Find the missing s_no values
missing_s_no_values = set(range(start_s_no, end_s_no+1)) - s_no_values

# Print the missing s_no values
if missing_s_no_values:
    print(f"Missing s_no values for station {station_id}: {missing_s_no_values}")
else:
    print(f"All s_no values present for station {station_id}")