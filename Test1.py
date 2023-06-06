from sqlalchemy import create_engine, text
from sshtunnel import SSHTunnelForwarder
import pandas as pd
import boto3
from datetime import datetime
import os


ssh_host = '43.207.203.155'
ssh_port = 22
ssh_user = 'ubuntu'
ssh_pkey = '/Users/mac/Desktop/AnalyticsNinja/aws-terraform-resources-startups/my_key_pair.pem'
db_user = 'admin'
db_password = 'password'
db_host = 'my-db-instance.cxjkxlbvg4uh.ap-northeast-1.rds.amazonaws.com'
db_name = 'information_schema'

bastion_server = SSHTunnelForwarder(
    (ssh_host, ssh_port),
    ssh_username=ssh_user,
    ssh_pkey=ssh_pkey,
    remote_bind_address=(db_host, 3306),
)

bastion_server.start()
local_db_port = bastion_server.local_bind_port

db_url = f'mysql+pymysql://{db_user}:{db_password}@127.0.0.1:{local_db_port}/{db_name}'

engine = create_engine(db_url)

conn = engine.connect()

# Get all data
result = conn.execute(text("SELECT * FROM ENGINES")).fetchall()

# Get column name
col = conn.execute(text("SHOW COLUMNS FROM ENGINES")).fetchall()
col_list = [x[0] for x in col]

# Convert to dataframe
df = pd.DataFrame(result, columns=col_list)

# Get current date for file path
today = str(datetime.now())[0:10].replace('-','')

# Get current directory and save as CSV
local_dir = os.getcwd()
file_dir =  '/engines_' + today + '.csv'
df.to_csv(local_dir + file_dir, sep=';', index = False)

# Upload to s3 bucket 
s3 = boto3.client('s3')
bucket_name = 'analytics-ninjas-rtw3hf'
s3.upload_file(local_dir + file_dir, bucket_name, file_dir.lstrip('/'))

conn.close()
bastion_server.stop()