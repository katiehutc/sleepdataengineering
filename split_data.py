import pandas as pd

df = pd.read_csv("Sleep_dataset.csv")
n = 4
splits = np.array_split(df, n)

for i, split in enumerate(splits, 1):
    split.to_csv(f"data_part{i}.csv", index=False)
