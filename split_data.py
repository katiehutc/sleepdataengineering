import pandas as pd

data = pd.read_csv("owid-covid-data.csv")

for continent, group in data.groupby(['Continent']):
    group.to_csv(f"{continent}.csv", index=False)
