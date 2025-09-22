import duckdb
import matplotlib.pyplot as plt


# connect to duckDB and load the parquet file
conn = duckdb.connect()
conn.execute("INSTALL httpfs")
conn.execute("LOAD httpfs")

conn.execute("SET s3_region='us-east-1'")
conn.execute("SET s3_access_key_id='xxxxxxxxxxxxxxxxx'")
conn.execute("SET s3_secret_access_key='xxxxxxxxxxxxxxxxxx'")

# query data
continents = {
    "Africa": "s3://covid-data/africa/*.parquet",
    "Europe": "s3://covid-data/europe/*.parquet",
    "Asia": "s3://covid-data/asia/*.parquet",
    "Americas": "s3://covid-data/americas/*.parquet",
    "Oceania": "s3://covid-data/oceania/*.parquet",
}

# compare the number of new cases and deaths by date in each continent
trend_dfs = {}
for name, path in continents.items():
    df = conn.execute(f"""
        SELECT date,
               SUM(new_cases)  AS daily_cases,
               SUM(new_deaths) AS daily_deaths
        FROM read_parquet('{path}')
        GROUP BY date
        ORDER BY date
    """).fetchdf()
    
    trend_dfs[name] = df

#visualize
fig, axes = plt.subplots(5, 1, figsize=(12, 20), sharex=True)

for i, (continent, df) in enumerate(trend_dfs.items()):
    axes[i].plot(df['date'], df['daily_cases'], label='Daily Cases', color='blue')
    axes[i].plot(df['date'], df['daily_deaths'], label='Daily Deaths', color='red')
    axes[i].set_title(continent)
    axes[i].set_ylabel("Count")
    axes[i].legend()

plt.xlabel("Date")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# get top 5 countries with most cases by continent
top_countries = {}
for name, path in continents.items():
    df = conn.execute(f"""
          SELECT country,
                MAX(total_cases) as total_cases
            FROM read_parquet('{path}')
            GROUP BY country
            ORDER BY total_cases DESC
            LIMIT 5
         """).fetchdf()
    top_countries[name] = df

#visualize
for continent, df in top_countries.items():
    df.plot.bar(x='country', y='total_cases', legend=False)
    plt.title(f"Top 10 Countries by Total COVID Cases ({continent})")
    plt.ylabel("Total Cases")
    plt.xticks(rotation=45)
    plt.show()

