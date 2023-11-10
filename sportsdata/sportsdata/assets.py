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


current_directory = os.getcwd()
database_file = os.path.join(current_directory, "my_duckdb_database.duckdb")


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


defs = Definitions(
    assets=[league_standing, get_scores,
            create_scores_table, create_standings_table],
    resources={
        "duckdb": DuckDBResource(
            database=database_file)}
)
