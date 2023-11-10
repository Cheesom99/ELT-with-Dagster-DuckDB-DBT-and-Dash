# ELT-with-Dagster-DuckDB-DBT-and-Dash

## Project Steps
- Create project folder
- Go to the folder directory
- Create a virtual environment and activate it
- Install dagster and dagster webserver
```bash
pip install dagster dagster-webserver
```
- Create Dagster Project
```bash
dagster project scaffold --name my-dagster-project (or any name you want for your project)
```
- Edit the setup.py
```python
from setuptools import find_packages, setup

setup(
    name="sports",
    packages=find_packages(exclude=["sports_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster",
        "dbt-duckdb",
        "dash",
        "duckdb",
        "bs4",
        "pandas",
        "requests",
        "dagster-webserver",
        "lxml",
        "dagit",
        "dagster-duckdb",
        "dagster-duckdb-pandas"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
```
- Install dependencies in setup.py
```python
pip install -e ".[dev]"
```
- Import libraries and define assets in assets.py
```python
import requests
from bs4 import BeautifulSoup
import pandas as pd
from dagster import AssetExecutionContext, asset
from dagster_duckdb import DuckDBResource
from dagster import Definitions
import os


@asset
def league_standing():
    urls = [
        {"url": "https://www.skysports.com/ligue-1-table", "source": "Ligue 1"},
        {"url": "https://www.skysports.com/premier-league-table",
            "source": "Premier League"},
        {"url": "https://www.skysports.com/la-liga-table", "source": "la liga"},
        {"url": "https://www.skysports.com/bundesliga-table", "source": "Bundesliga"},
        {"url": "https://www.skysports.com/serie-a-table", "source": "Seria A"},
        {"url": "https://www.skysports.com/eredivisie-table", "source": "Eredivisie"},
        {"url": "https://www.skysports.com/scottish-premier-table",
            "source": "Scottish premiership"}
    ]
    dfs = []

    for url_info in urls:
        url = url_info["url"]
        source = url_info["source"]

        # Send HTTP Request and Parse HTML
        r = requests.get(url)
        soup = BeautifulSoup(r.text, "lxml")

        # Find and Extract Table Headers
        table = soup.find("table", class_="standing-table__table")
        headers = table.find_all("th")
        titles = [i.text for i in headers]

        # Create an Empty DataFrame
        df = pd.DataFrame(columns=titles)

        # Iterate Through Table Rows and Extract Data
        rows = table.find_all("tr")
        for i in rows[1:]:
            data = i.find_all("td")
            # Apply .strip() to remove \n
            row = [tr.text.strip() for tr in data]
            l = len(df)
            df.loc[l] = row

        # Add a column for source URL
        df["Source"] = source

        # Append the DataFrame to the list
        dfs.append(df)

    # Concatenate all DataFrames into a single DataFrame
    football_standing = pd.concat(dfs, ignore_index=True)
    football_standing.to_csv("footballstanding.csv")


@asset
def get_scores():
    url = "https://www.skysports.com/football-results"
    r = requests.get(url)
    soup = BeautifulSoup(r.text, "lxml")

    home_team = soup.find_all(
        "span", class_="matches__item-col matches__participant matches__participant--side1")
    x = [name.strip() for i in home_team for name in i.stripped_strings]

    scores = soup.find_all("span", class_="matches__teamscores")
    s = [name.strip().replace('\n\n', '\n')
         for i in scores for name in i.stripped_strings]
    appended_scores = [
        f"{s[i]}\n{s[i+1]}".replace('\n', ' ') for i in range(0, len(s), 2)]

    away_team = soup.find_all(
        "span", class_="matches__item-col matches__participant matches__participant--side2")
    y = [name.strip() for i in away_team for name in i.stripped_strings]

    # Make sure all arrays have the same length
    min_length = min(len(x), len(appended_scores), len(y))
    data = {"Home Team": x[:min_length],
            "Scores": appended_scores[:min_length], "Away Team": y[:min_length]}
    footballscores = pd.DataFrame(data)
    footballscores.to_csv("footscores.csv")
```
- Start dagster server
```bash
dagster dev
```
- Check your dagster server at http://127.0.0.1:3000/ and materialize your assets. If the materialization is successful, you should see two files in your folder named footballstanding.csv and footscores.csv
- Create a file path for the DuckDB database file (still on assets.py)
```python
current_directory = os.getcwd()
database_file = os.path.join(current_directory, "my_duckdb_database.duckdb")
```
- Create the Tables
```python
@asset
def create_scores_table(duckdb: DuckDBResource) -> None:
    sports_df = pd.read_csv(
        "footscores.csv",
        names=['Unnamed: 0',
               'Home Team',
               'Scores',
               'Away Team'],
    )

    with duckdb.get_connection() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS scores AS SELECT * FROM sports_df")

@asset
def create_standings_table(duckdb: DuckDBResource) -> None:
    standings_df = pd.read_csv("footballstanding.csv",
                               names=["Unnamed: 0",
                                      "#",
                                      "Team",
                                      "Pl",
                                      "W",
                                      "D",
                                      "L",
                                      "F",
                                      "A",
                                      "GD",
                                      "Pts",
                                      "Last 6",
                                      "Source"])

    with duckdb.get_connection() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS standings AS SELECT * FROM standings_df")
```
Start dagster server and materialize new assets.
```bash
dagster dev
```
- If the above fails edit the init.py file in the same folder as your assets.py. Note there are two init.py files
```python
from . import assets
from dagster import Definitions, load_assets_from_modules
from dagster_duckdb import DuckDBResource
from dagster import Definitions
import os

current_directory = os.getcwd()
database_file = os.path.join(current_directory, "my_duckdb_database.duckdb")


all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    resources={
        "duckdb": DuckDBResource(
            database=database_file,
        )
    },
)
```

- Initialize your dbt project
```bash
dbt init project_name
```
- Create profiles.yml file in the dbt folder just created
```
sports_dbt:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "path_to/my_duckdb_database.duckdb"
```
- Run dbt debug
- In a separate file (test.py), check if 2 tables exist in the database
```python
import duckdb

conn = duckdb.connect('my_duckdb_database.duckdb')
# Execute the SQL query to list tables
result = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")

# Fetch and print the table names
table_names = result.fetchall()
for table_name in table_names:
    print(table_name[0])
```
- Copy the two files into the seeds folder (footballstanding.csv and footscores.csv)
- Run dbt seed to load the files into the data warehouse

- Wrote a SQL script in the models folder to query data from the footballstanding table
```sql
WITH team_points AS (
    SELECT 
        Team,
        SUM(Pts) AS Total_points
    FROM {{ref('footballstanding')}}
    GROUP BY Team
)

SELECT 
    f.team AS team,
    f.Pts AS points,
    f.Pl AS games_played,
    f.W AS win,
    f.D AS draw,
    f.L AS lost,
    f.F AS goals_for,
    f.Against AS goals_against,
    f.GD AS goal_difference,
    f.Last_6 AS last_6_results,
    f.Source AS league,
    t.total_points AS total_points

FROM {{ref('footballstanding')}} AS f
JOIN team_points AS t ON f.Team = t.Team
ORDER BY t.total_points DESC
```
- Modified schema.yml and ran dbt build to execute the entire dbt project.

### For Plotly Dashboard
- Import librarues and connect to the database
```python
from dash import Dash, html, dcc, dash_table, callback, Output, Input
import plotly.express as px
import pandas as pd
import duckdb

conn = duckdb.connect('my_duckdb_database.duckdb')
query = ('select * from standings')
data = conn.execute(query).df()
conn.close()

app = Dash(__name__)
# Define the layout etc here

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)
```


Cheesom99/ELT-with-Dagster-DuckDB-DBT-and-Dash
