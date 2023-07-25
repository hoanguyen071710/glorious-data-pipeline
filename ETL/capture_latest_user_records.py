import boto3
import pandas as pd
from sqlalchemy import create_engine, text
import datetime
import os
import psycopg2
import numpy as np

db_url = os.environ["YDB_URL"]
engine = create_engine(db_url)


def check_no_rows_dw(conn):
    query = """
    SELECT *
    FROM user_dim
    LIMIT 1
    """
    result = conn.execute(text(query)).fetchall()
    return result


def get_latest_snapshot_datalake(days=1):
    snap_dir = get_path_snapshot(days)
    s3_client = boto3.client("s3")
    s3_bucket = "analytics-ninjas"
    s3_object_key = snap_dir.lstrip("/")
    s3_response = s3_client.get_object(Bucket=s3_bucket, Key=s3_object_key)
    df = pd.read_csv(s3_response["Body"])
    return df


def get_path_snapshot(days):
    today = str(
        datetime.datetime.now()
        + datetime.timedelta(hours=7)
        - datetime.timedelta(days=days)
    )[0:10].replace("-", "")
    return f"/stock_db/user/partition={today}/user.csv"


def get_records(df, records_type):
    latest_date = latest_date = (
        datetime.datetime.now()
        + datetime.timedelta(hours=7)
        - datetime.timedelta(days=1)
    ).strftime("%Y-%m-%d")
    df = df.loc[df["updated_at"].str.split(" ").str[0] == latest_date, :]
    if records_type != "All":
        df = df.loc[df["status"] == records_type]
    df.rename(columns={"updated_at": "start_date"})
    df = df.drop(columns=["status"])
    df["end_date"] = "9999-12-31 23:59:59"
    df["is_current"] = True
    vectorized_element_to_string = np.vectorize(str)
    records = ",".join(
        vectorized_element_to_string(pd.Series(df.to_records(index=False).tolist()))
    )
    return df, records


def update_end_date_outdated_records(conn, df):
    update_values = get_records(df, "U")[1]
    query = f"""
    INSERT INTO user_dim (
        user_id, name, email, start_date, end_date, is_current
    ) 
    VALUES {update_values}
    ON CONFLICT (user_id, end_date) DO 
    UPDATE 
    SET 
        end_date = EXCLUDED.start_date - interval '1 second',
        is_current = false 
    WHERE 
        user_dim.sk_user_id = (
            SELECT 
                MAX(sk_user_id) 
            FROM 
                user_dim 
            WHERE 
                user_id = EXCLUDED.user_id
    );
    """
    conn.execute(text(query))
    conn.commit()


def insert_new_records(conn, df):
    insert_values = get_records(df, "All")[1]
    query = f"""
    INSERT INTO user_dim (
        user_id, name, email, start_date, end_date, is_current
    ) 
    VALUES {insert_values}
    """
    conn.execute(text(query))


def incremental_load(conn, df):
    update_end_date_outdated_records(conn, df)
    insert_new_records(conn, df)


def full_load(conn, df):
    insert_new_records(conn, df)


if __name__ == "__main__":
    with engine.connect() as conn:
        df = get_latest_snapshot_datalake()
        if check_no_rows_dw(conn):
            incremental_load(conn, df)
        else:
            full_load(conn, df)
        conn.commit()
