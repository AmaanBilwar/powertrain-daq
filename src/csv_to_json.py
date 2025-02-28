import pandas as pd

def csv_to_json(csv_file_path):
    csv_file_path=pd.DataFrame(pd.read_csv("decoded_can_messages.csv", sep=","))
    csv_file_path.to_json("decoded_can_messages.json", force_ascii=True, orient='records')
    return "Complete. Return to the InfluxDB UI."
