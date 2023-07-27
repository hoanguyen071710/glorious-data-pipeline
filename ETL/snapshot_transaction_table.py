from sqlalchemy import create_engine, text
import pandas as pd
import datetime
import sys
import boto3
from io import StringIO
import os

db_url = os.environ["DB_URL"]
engine = create_engine(db_url)


def check_command_line_argv(default_value=0):
    arg_value = default_value
    if len(sys.argv) > 1:
        arg_value = sys.argv[1]
    return int(arg_value)


def get_latest_records(conn):
    transaction_date = (
        datetime.datetime.now()
        + datetime.timedelta(hours=7)
        - datetime.timedelta(days=check_command_line_argv())
    ).strftime("%Y-%m-%d")
    query = f"""
    SELECT *
    FROM Transaction
    WHERE DATE(transaction_date) = DATE '{transaction_date}' - 1
    """
    result = conn.execute(text(query)).fetchall()
    return result


def get_path_snapshot(days=2, append_file=True):
    today = str(
        datetime.datetime.now()
        + datetime.timedelta(hours=7)
        - datetime.timedelta(days=check_command_line_argv())
        - datetime.timedelta(days=days)
    )[0:10].replace("-", "")
    bucket_path = f"/stock_db/transaction/partition={today}/transaction.csv"
    return bucket_path if append_file else bucket_path.rstrip("/transaction.csv")


def get_full_records(conn):
    current_date = (
        datetime.datetime.now()
        + datetime.timedelta(hours=7)
        - datetime.timedelta(days=check_command_line_argv())
    ).strftime("%Y-%m-%d")
    query = f"""
    SELECT *
    FROM Transaction
    WHERE DATE(transaction_date) < '{current_date}'
    """
    result = conn.execute(text(query)).fetchall()
    return pd.DataFrame(result)


def upload_to_datalake(df):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    file_dir = get_path_snapshot(1)
    s3 = boto3.client("s3")
    s3_bucket = "analytics-ninjas"
    s3.put_object(
        Body=csv_buffer.getvalue(), Bucket=s3_bucket, Key=file_dir.lstrip("/")
    )
    csv_buffer.close()


def full_load():
    with engine.connect() as conn:
        upload_to_datalake(get_full_records(conn))


if __name__ == "__main__":
    full_load()
