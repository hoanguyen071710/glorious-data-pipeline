import boto3
import pandas as pd
from sqlalchemy import create_engine, text
import datetime
from io import StringIO
import os
import sys

db_url = os.environ["DB_URL"]
engine = create_engine(db_url)


def check_command_line_argv(default_value=0):
    arg_value = default_value
    if len(sys.argv) > 1:
        arg_value = sys.argv[1]
    return int(arg_value)


def get_latest_records(conn):
    updated_at = (
        datetime.datetime.now()
        + datetime.timedelta(hours=7)
        - datetime.timedelta(days=check_command_line_argv())
    ).strftime("%Y-%m-%d")
    query = f"""
    SELECT *
    FROM User
    WHERE DATE(updated_at) = DATE '{updated_at}' - 1
    """
    result = conn.execute(text(query)).fetchall()
    return result


def get_latest_snapshot_datalake():
    snap_dir = get_path_snapshot()
    s3_client = boto3.client("s3")
    s3_bucket = "analytics-ninjas"
    s3_object_key = snap_dir.lstrip("/")
    s3_response = s3_client.get_object(Bucket=s3_bucket, Key=s3_object_key)
    df = pd.read_csv(s3_response["Body"])
    return df


def get_path_snapshot(days=2, append_file=True):
    today = str(
        datetime.datetime.now()
        + datetime.timedelta(hours=7)
        - datetime.timedelta(days=check_command_line_argv())
        - datetime.timedelta(days=days)
    )[0:10].replace("-", "")
    bucket_path = f"/stock_db/user/partition={today}/user.csv"
    return bucket_path if append_file else bucket_path.rstrip("/user.csv")


def update_snapshot(latest_records, latest_snapshot):
    update_records = latest_records.loc[latest_records["status"] == "U"]
    update_records_id = update_records["user_id"].values
    latest_snapshot = latest_snapshot.loc[
        ~latest_snapshot["user_id"].isin(update_records_id)
    ]
    return pd.concat([latest_snapshot, update_records]).sort_values(by="user_id")


def insert_snapshot(latest_records, latest_snapshot):
    insert_records = latest_records.loc[latest_records["status"] == "I"]
    return pd.concat([latest_snapshot, insert_records]).sort_values(by="user_id")


def get_full_records(conn):
    current_date = (
        datetime.datetime.now()
        + datetime.timedelta(hours=7)
        - datetime.timedelta(days=check_command_line_argv())
    ).strftime("%Y-%m-%d")
    query = f"""
    SELECT *
    FROM User
    WHERE DATE(updated_at) < '{current_date}'
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


def incremental_load():
    with engine.connect() as conn:
        latest_records = get_latest_records(conn)
        latest_snapshot = get_latest_snapshot_datalake()
        upload_to_datalake(
            insert_snapshot(
                latest_records, update_snapshot(latest_records, latest_snapshot)
            )
        )


def list_s3_files(bucket_name, prefix=""):
    s3 = boto3.client("s3")
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = []
    for obj in response.get("Contents", []):
        files.append(obj["Key"])
    return files


def full_load():
    with engine.connect() as conn:
        upload_to_datalake(get_full_records(conn))


if __name__ == "__main__":
    if list_s3_files("analytics-ninjas", get_path_snapshot(append_file=False)):
        incremental_load()
    else:
        full_load()
