import pandas as pd
df = pd.read_csv("owid-covid-data.csv")

for continent, group_df in df.groupby('continent'):
    filename = f"{continent}.csv"  
    group_df.to_csv(filename, index=False)
    print("filename")