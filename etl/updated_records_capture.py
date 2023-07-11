from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
import os
import boto3


# Get the current date
current_date = datetime.now().date()

# Calculate the n-1 day's date
n_1_date = str(current_date - timedelta(days=1)).replace('-','')

# Calculate the n-2 day's date
n_2_date = str(current_date - timedelta(days=2)).replace('-','')

# Download the file into a buffer
bucket_name = 'analytics-ninjas'
csv_buffer1 = StringIO()
csv_buffer2 = StringIO()
file_dir_1 =  '/etl_' + n_1_date + '.csv'
file_dir_2 =  '/etl_' + n_2_date + '.csv'
s3 = boto3.client('s3',aws_access_key_id=os.environ["ACCESS_KEY"], aws_secret_access_key=os.environ["SECRET_KEY"])
s3.download_fileobj(bucket_name, file_dir_1.lstrip('/'), csv_buffer1)
s3.download_fileobj(bucket_name, file_dir_2.lstrip('/'), csv_buffer2)

# Read the CSV data from the buffer and convert it into a DataFrame
snap1_df = pd.read_csv(csv_buffer1)
snap2_df = pd.read_csv(csv_buffer2)

# Closing allocate memory
csv_buffer1.close()
csv_buffer2.close()

# Find updated records
updated_record = snap1_df.merge(snap2_df, on=['stock_id', 'company', 'category', 'price'], how='left', indicator=True)
updated_record = updated_record[updated_record['price_y'].isnull()]