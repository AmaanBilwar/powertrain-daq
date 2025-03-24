import os,time
from influxdb_client_3 import InfluxDBClient3, Point
import pandas as pd


token = os.environ.get("INFLUXDB_TOKEN")
org = "university of cincinnati"
host = "https://us-east-1-1.aws.cloud2.influxdata.com"

client = InfluxDBClient3(host=host, token=token, org=org)

database = "testcandata"

csv_file_path="decoded_can_messages.csv"
data = pd.read_csv(csv_file_path)


for key in data:
  point = (
    Point("can-data")
    .tag("timestamp", data[key]["timestamp"])
    .field(data[key]["species"], data[key]["count"])
  )
  client.write(database=database, record=point)
  time.sleep(1) # separate points by 1 second

print("Complete. Return to the InfluxDB UI.")
client.write(database=database, record=point)
time.sleep(1)

print("Complete. Return to the InfluxDB UI")
