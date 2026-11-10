import duckdb

con = duckdb.connect("dev.duckdb")
# שליפת הנתונים מטבלת ה-Snapshot שנוצרה
df = con.sql("SELECT id, name, email, dbt_valid_from, dbt_valid_to FROM main.snap_users").df()
print(df)