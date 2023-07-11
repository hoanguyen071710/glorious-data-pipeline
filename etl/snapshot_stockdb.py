from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
import os
import boto3

db_url = os.environ["DB_URL"]
engine = create_engine(db_url)
conn = engine.connect()

snapshot = "SELECT * FROM stock"
snapshot_data = conn.execute(text(snapshot)).fetchall()

# Get column name
col = conn.execute(text("SHOW COLUMNS FROM stock")).fetchall()
col_list = [x[0] for x in col]

# Convert to dataframe
snapshot_df = pd.DataFrame(snapshot_data, columns=col_list)

# Get the current date
current_date = datetime.now().date()

# Calculate the n-1 day's date
n_1_date = str(current_date - timedelta(days=1)).replace('-','')

# Save CSV to memory but not solid state disk
csv_buffer = StringIO()
snapshot_df.to_csv(csv_buffer, index = False)
file_upload_dir =  '/etl_' + n_1_date + '.csv'

# Upload to s3 bucket
s3 = boto3.client('s3',aws_access_key_id=os.environ["ACCESS_KEY"], aws_secret_access_key=os.environ["SECRET_KEY"])
bucket_name = 'analytics-ninjas'
s3.put_object(Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=file_upload_dir.lstrip('/'))

# Closing allocate memory, and database connection
csv_buffer.close()
conn.close()