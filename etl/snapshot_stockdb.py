from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from io import StringIO
import pandas as pd
import os
import boto3

db_url = os.environ["DB_URL"]
engine = create_engine(db_url)
conn = engine.connect()


def extract_data():
    snapshot = f"SELECT * FROM stock_db.Stock"
    snapshot_data = conn.execute(text(snapshot)).fetchall()
    col = conn.execute(text("SHOW COLUMNS FROM stock_db.Stock")).fetchall()
    col_list = [x[0] for x in col]
    snapshot_df = pd.DataFrame(snapshot_data, columns=col_list)
    return snapshot_df


def date_substraction(day):
    current_date = (datetime.now() + timedelta(hours=7)).date()
    n_1_date = str(current_date - timedelta(days=day)).replace("-", "")
    return n_1_date


def upload_to_s3(df, date):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    file_upload_dir = f"stock_db/stock/partition={date}/stock.csv"
    s3 = boto3.client("s3")
    bucket_name = "analytics-ninjas"
    s3.put_object(
        Body=csv_buffer.getvalue(), Bucket=bucket_name, Key=file_upload_dir.lstrip("/")
    )
    csv_buffer.close()
    conn.close()


if __name__ == "__main__":
    upload_to_s3(extract_data(), date_substraction(1))
