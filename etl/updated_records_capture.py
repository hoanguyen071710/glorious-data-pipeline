from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from io import BytesIO
import pandas as pd
import os
import boto3
import io

# Get the current date
current_date = datetime.now().date()

# Calculate the n-1 day's date
n_1_date = str(current_date - timedelta(days=1)).replace('-','')

# Calculate the n-2 day's date
n_2_date = str(current_date - timedelta(days=2)).replace('-','')

# Download the file into a buffer
bucket_name = 'analytics-ninjas'
csv_buffer1 = BytesIO()
csv_buffer2 = BytesIO()
file_dir_1 =  f'stock_db/stock/partition={n_1_date}/stock.csv'
file_dir_2 =  f'stock_db/stock/partition={n_2_date}/stock.csv'
s3 = boto3.client('s3',aws_access_key_id=os.environ["ACCESS_KEY"], aws_secret_access_key=os.environ["SECRET_KEY"])
s3.download_fileobj(Bucket=bucket_name, Key=file_dir_1, Fileobj=csv_buffer1)
s3.download_fileobj(Bucket=bucket_name, Key=file_dir_2, Fileobj=csv_buffer2)

# Read the binary data from the BytesIO object and decode it as a string
csv_1 = csv_buffer1.getvalue().decode('utf-8')
csv_2 = csv_buffer2.getvalue().decode('utf-8')

# Create a DataFrame from the CSV data
snap1_df = pd.read_csv(io.StringIO(csv_1))
snap2_df = pd.read_csv(io.StringIO(csv_2))

# Closing allocate memory
csv_buffer1.close()
csv_buffer2.close()

# Find updated records
updated_record = snap1_df.merge(snap2_df, on=['stock_id', 'company', 'category', 'price'], how='left', indicator=True)
updated_record = updated_record[updated_record['_merge'] == 'left_only']
updated_record = updated_record.loc[:, ['stock_id', 'company', 'category', 'price']]

# Connect Yugabyte DB
db_url = os.environ["YDB_URL"]
engine = create_engine(db_url)
conn = engine.connect()

# Insert updated record
updated_record.to_sql('stock_dim', conn, if_exists='append', index=False)