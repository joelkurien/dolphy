from cleanerpl import CleanerPipeline
import polars as pl
import os

filepath = os.path.expanduser("~/storage/inbound/citadel_alltime.csv")
print(os.path.exists(filepath))

with open(filepath) as f:
    df = pl.read_csv(f)

print(df.head(3))
px = CleanerPipeline(df)
px.filter("(Difficulty == 'Medium') and (Acceptance == '49.40%')")
df = px.execute()
print(df.head(3))
