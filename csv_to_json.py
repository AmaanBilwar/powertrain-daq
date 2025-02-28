import pandas as pd


csv_file_path=pd.DataFrame(pd.read_csv("decoded_can_messages.csv", sep=","))
csv_file_path.to_json("decoded_can_messages.json", force_ascii=True, orient='records')
print("Complete. Return to the InfluxDB UI.")

