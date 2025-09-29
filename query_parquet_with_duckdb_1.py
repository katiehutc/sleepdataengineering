import duckdb
import os
import matplotlib.pyplot as plt


# connect to duckDB and load the parquet file
conn = duckdb.connect()
conn.execute("INSTALL httpfs")
conn.execute("LOAD httpfs")

conn.execute("SET s3_region='eu-north-1'")
#conn.execute("SET s3_access_key_id='ljP08kCCura/88ZsNrOZbw4ukZkIDLoEthIedxaY'")
#conn.execute("SET s3_secret_access_key='AKIAW6TYJHWIDG7UNMVE'")

# query data
continents = {
    "Africa": "s3://ds-3021-project1/parquet/Africa.parquet",
    "Europe": "s3://ds-3021-project1/parquet/Europe.parquet",
    "Asia": "s3://ds-3021-project1/parquet/Asia.parquet",
    #"Americas": "s3://ds-3021-project1/mericas/*.parquet",
    "Oceania": "s3://ds-3021-project1/parquet/Oceania.parquet",
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
plt.savefig("trend_by_continent.png")

# get top 5 countries with most cases by continent
top_countries = {}
for name, path in continents.items():
    df = conn.execute(f"""
          SELECT location,
                MAX(total_cases) as total_cases
            FROM read_parquet('{path}')
            GROUP BY location
            ORDER BY total_cases DESC
            LIMIT 5
         """).fetchdf()
    top_countries[name] = df

#visualize
for continent, df in top_countries.items():
    df.plot.bar(x='location', y='total_cases', legend=False)
    plt.title(f"Top 10 Countries by Total COVID Cases ({continent})")
    plt.ylabel("Total Cases")
    plt.xticks(rotation=45)
    filename = f"top10_{continent.lower()}.png"
    plt.savefig(filename)
    plt.close() 

#compare the case fatality rate (CFR)
continents_list = []
cfr_values = []

for continent, path in continents.items():
    total_cases, total_deaths, cfr = conn.execute(f"""
        SELECT
            MAX(total_cases) AS total_cases,
            MAX(total_deaths) AS total_deaths,
            ROUND(MAX(total_deaths) * 100.0 / NULLIF(MAX(total_cases), 0), 2) AS cfr
        FROM read_parquet('{path}')
    """).fetchone()
    
    continents_list.append(continent)
    cfr_values.append(cfr)

#visualize
plt.figure(figsize=(8,6))
plt.bar(continents_list, cfr_values, color="orange")
plt.title("COVID-19 Case Fatality Rate (CFR) by Continent")
plt.ylabel("CFR (%)")
plt.xlabel("Continent")
plt.tight_layout()
output_path = os.path.join(os.getcwd(), "cfr_by_continent.png")
plt.savefig(output_path)
plt.close()
