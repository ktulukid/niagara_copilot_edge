from pprint import pprint
from .csv_history_client import CsvHistoryClient

client = CsvHistoryClient("./data")

samples = client.get_history("vav_1_01_space_temp", hours=24)

for s in samples[:10]:
    pprint(s.model_dump())
