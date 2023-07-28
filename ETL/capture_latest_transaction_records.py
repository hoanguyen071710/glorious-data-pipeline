from sqlalchemy import create_engine, text
import boto3
import pandas as pd
import datetime
import numpy as np
import os

db_url = os.environ["YDB_URL"]
engine = create_engine(db_url)


def check_no_rows_dw(conn):
    query = """
    SELECT *
    FROM transaction_fact
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
    return f"/stock_db/transaction/partition={today}/transaction.csv"


def get_records_incremental_load(df, days=1):
    latest_date = (
        datetime.datetime.now()
        + datetime.timedelta(hours=7)
        - datetime.timedelta(days=days)
    ).strftime("%Y-%m-%d")
    df = df.loc[df["transaction_date"].str.split(" ").str[0] == latest_date, :]
    return df


def records_to_tuple(df):
    vectorized_element_to_string = np.vectorize(str)
    records = ",".join(
        vectorized_element_to_string(pd.Series(df.to_records(index=False).tolist()))
    )
    return df, records


def get_dim_key(conn, dim_name):
    sk = f"sk_{dim_name}_id"
    nk = f"{dim_name}_id"
    query = f"""
    SELECT {sk}, {nk}
    FROM {dim_name}_dim
    WHERE is_current = True
    """
    result = conn.execute(text(query)).fetchall()
    result_df = pd.DataFrame(result, columns=[sk, nk])
    return result_df


def get_user_sk(conn, load_type, df=get_latest_snapshot_datalake()):
    transaction_source = df if load_type == "f" else get_records_incremental_load(df)
    user_dim = get_dim_key(conn, "user")
    stock_dim = get_dim_key(conn, "stock")
    fact_df = pd.merge(
        left=transaction_source,
        right=user_dim,
        left_on="user_id",
        right_on="user_id",
        how="left",
    )
    fact_df = pd.merge(
        left=fact_df,
        right=stock_dim,
        left_on="stock_id",
        right_on="stock_id",
        how="left",
    )
    fact_df = fact_df.rename(columns={"sk_user_id": "FK_user_id"})
    fact_df = fact_df.rename(columns={"sk_stock_id": "FK_stock_id"})
    fact_df = fact_df[["FK_user_id", "FK_stock_id", "quantity", "transaction_date"]]
    return fact_df


def insert_records(conn, fact_df):
    insert_values = records_to_tuple(fact_df)[1]
    query = f"""
    INSERT INTO transaction_fact (FK_user_id, FK_stock_id, quantity, transaction_date)
    VALUES {insert_values}
    """
    conn.execute(text(query))


def incremental_load(conn):
    fact_df = get_user_sk(conn, "i")
    insert_records(conn, fact_df)


def full_load(conn):
    fact_df = get_user_sk(conn, "f")
    insert_records(conn, fact_df)


if __name__ == "__main__":
    with engine.connect() as conn:
        if check_no_rows_dw(conn):
            incremental_load(conn)
        else:
            full_load(conn)
        conn.commit()
