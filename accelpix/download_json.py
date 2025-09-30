import requests
import pandas as pd

url = "https://apidata5.accelpix.in/api/hsd/Masters/2?fmt=json"

result = requests.get(url)
data = result.json()

df = pd.DataFrame(data)

# Drop rows where inst == EQUITY
df = df[df["inst"] != "EQUITY"]

# Save to JSON
df.to_json("instru.json", orient="records", indent=2)
