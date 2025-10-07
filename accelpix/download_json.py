import requests
import pandas as pd
from configparser import ConfigParser
config = ConfigParser()
config.read("/root/ParallelCandles/config.ini")

url = config.get("accelpix", "instruments_endpoint")
result = requests.get(url)
data = result.json()
df = pd.DataFrame(data)

# Drop rows where inst == EQUITY
df = df[df["inst"] != "EQUITY"]

# Save to JSON
df.to_json("instru.json", orient="records", indent=2)
