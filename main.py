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

delta = timedelta(days=1)
start_date = latest_date + delta
end_date = date.today()
print(f"Starting {start_date} to End {end_date} ")

date_list_1=[]

while start_date < end_date:
    date_list_1.append(start_date)
    start_date += delta

# Create the datasets for each date
for i in date_list_1:
    try:
        date_list = t.list(date=i)
        print(f'Date List - {date_list}')
        j = date_list.list_id
        url = 'https://tranco-list.eu/download_daily/'+str(j)
        print(url)
        df = pd.read_csv(url,compression='zip', header=None)
        df.columns = ['rank', 'domain']
        df['date'] = i
        df = df[["date", "domain", "rank"]]
        print(df)
        df.to_gbq(
            'webtraffic-363823.tranco.historical',
            project_id='webtraffic-363823',
            if_exists='append',
            table_schema=[
                {'name': 'int64_field_0', 'type': 'INTEGER'},
                {'name': 'date', 'type': 'DATE'},
                {'name': 'domain', 'type': 'STRING'},
                {'name': 'rank', 'type': 'INTEGER'},
            ],
            progress_bar=True,
            credentials=None
        )
    except Exception as e:
        print(f"Failed to continue: {e}")
