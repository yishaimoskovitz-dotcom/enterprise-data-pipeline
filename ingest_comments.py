import requests
import pandas as pd
import duckdb
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

url1 = 'https://jsonplaceholder.typicode.com/users'
url2 = 'https://jsonplaceholder.typicode.com/comments'

response1 = requests.get(url1, verify=False)
response2 = requests.get(url2, verify=False)

df1 = pd.DataFrame(response1.json())
df2 = pd.DataFrame(response2.json())

con = duckdb.connect("dev.duckdb")
con.execute("CREATE OR REPLACE TABLE raw_users AS SELECT * FROM df1")
con.execute("CREATE OR REPLACE TABLE raw_comments AS SELECT * FROM df2")

