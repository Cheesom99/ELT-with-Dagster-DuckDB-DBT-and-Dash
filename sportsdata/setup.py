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
