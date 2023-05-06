# # importing the libraries used
# import pandas as pd
#
# # initializing the data
# data = {
#     "station_id": 1,
#     "s_no": 1,
#     "battery_status": "low",
#     "status_timestamp": 1681521224,
#     "weather": {
#         "humidity": 35,
#         "temperature": 100,
#         "wind_speed": 13
#     }
# }
# df = pd.json_normalize(data)
# df = pd.json_normalize(data,max_level=0)
# df.to_parquet("out.parquet")
from datetime import datetime

from datetime import datetime
current_date = datetime.now()
print(current_date.strftime('%Y-%m-%dT%H:%M:%S.%f%z'))

