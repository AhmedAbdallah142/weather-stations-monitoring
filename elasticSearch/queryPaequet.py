import pyarrow.parquet as pq
import os

# Specify the path to the Parquet file
parquet_file_path = "C:\\Users\\Kimo Store\\Desktop\\weather-stations-monitoring\\centralStation\\archive"

# Initialize a counter for the total number of rows
total_rows = 0

for (dirpath, _, filenames) in os.walk(parquet_file_path):
    for file in filenames:
        if file.endswith("crc"):
            continue
        filename = os.path.basename(file)[:-len(".parquet")]
        file_timestamp = int(filename)
        if file.endswith(".parquet"):
            # Create a ParquetDataset object
            file_path = os.path.join(dirpath, file)
            dataset = pq.ParquetFile(file_path)

            # Get the number of row groups in the dataset
            num_row_groups = dataset.num_row_groups



            # Iterate over the row groups and sum up the number of rows
            for i in range(num_row_groups):
                row_group = dataset.read_row_group(i)
                total_rows += row_group.num_rows

# Print the total number of rows
print("Total number of rows:", total_rows)