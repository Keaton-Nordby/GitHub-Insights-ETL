import pandas as pd
import json

with open("reports/daily_summary.json") as f:
    data = json.load(f)

df = pd.DataFrame(data)
print(df.head(10))  # top 10 repos
