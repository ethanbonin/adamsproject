from google.cloud import bigquery
from tranco import Tranco
from datetime import date, timedelta
import pandas as pd

t=Tranco(cache=True, cache_dir='.tranco')


# Construct a BigQuery client object.
client = bigquery.Client()

query = """
    SELECT date 
    FROM `webtraffic-363823.tranco.historical` 
    WHERE date IN (SELECT MAX(date) FROM `webtraffic-363823.tranco.historical`) LIMIT 1
"""

## Grab the latest Date
latest_date = None
query_job = client.query(query)  # Make an API request.
for row in query_job:
    # Row values can be accessed by field name or index.
    print("latest date entry={}".format(row[0]))
    latest_date = row[0]


start_date = latest_date
end_date = date.today()
delta = timedelta(days=1)
date_list_1=[]

while start_date < end_date:
    date_list_1.append(start_date)
    start_date += delta

# Create the datasets for each date
for i in date_list_1:
    try:
        date_list = t.list(date=i)
        j = date_list.list_id
        url = 'https://tranco-list.eu/download_daily/'+str(j)
        print(url)
        df = pd.read_csv(url,compression='zip',header=None)
        df.columns = ['index', 'rank', 'domain']
        df['date'] = i
        df = df[["index", "date", "domain", "rank"]]

        # https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe
        # https://medium.com/@larry_nguyen/load-data-into-gcp-bigquery-table-using-pandas-dataframe-7f04260ef518
        # https://kontext.tech/article/682/pandas-save-dataframe-to-bigquery
        # TODO: Need to write query to INPUT DATA INTO BIG QUERY
    except:continue
