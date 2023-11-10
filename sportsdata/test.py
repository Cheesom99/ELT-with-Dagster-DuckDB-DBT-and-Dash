import duckdb

conn = duckdb.connect('my_duckdb_database.duckdb')
# Execute the SQL query to list tables
result = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")

# Fetch and print the table names
table_names = result.fetchall()
for table_name in table_names:
    print(table_name[0])

# query = ("select * from standings")
# result = conn.execute(query).df()
# print(result)

# conn.execute("select * from scores").fetchall()
