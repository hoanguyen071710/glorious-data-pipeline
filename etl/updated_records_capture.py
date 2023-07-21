from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from io import BytesIO
import pandas as pd
import os
import boto3
import io


def date_substraction(day1, day2):
    current_date = datetime.now().date()
    n_1_date = str(current_date - timedelta(days=day1)).replace("-", "")
    n_2_date = str(current_date - timedelta(days=day2)).replace("-", "")
    return n_1_date, n_2_date


def down_file(n_1_date, n_2_date):
    bucket_name = "analytics-ninjas"
    csv_buffer1 = BytesIO()
    csv_buffer2 = BytesIO()
    file_dir_1 = f"stock_db/stock/partition={n_1_date}/stock.csv"
    file_dir_2 = f"stock_db/stock/partition={n_2_date}/stock.csv"
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.environ["ACCESS_KEY"],
        aws_secret_access_key=os.environ["SECRET_KEY"],
    )
    s3.download_fileobj(Bucket=bucket_name, Key=file_dir_1, Fileobj=csv_buffer1)
    s3.download_fileobj(Bucket=bucket_name, Key=file_dir_2, Fileobj=csv_buffer2)
    csv_1 = csv_buffer1.getvalue().decode("utf-8")
    csv_2 = csv_buffer2.getvalue().decode("utf-8")
    csv_buffer1.close()
    csv_buffer2.close()
    return csv_1, csv_2


def convert_to_df(csv1, csv2):
    snap1_df = pd.read_csv(io.StringIO(csv1))
    snap2_df = pd.read_csv(io.StringIO(csv2))
    return snap1_df, snap2_df


def find_updated_records(df1, df2):
    updated_record = df1.merge(
        df2, on=["stock_id", "company", "category", "price"], how="left", indicator=True
    )
    updated_record = updated_record[updated_record["_merge"] == "left_only"]
    updated_record = updated_record.loc[:, ["stock_id", "company", "category", "price"]]
    updated_record["start_date"] = str(datetime.now().date()) + " 08:00:00"
    updated_record["end_date"] = "9999-01-01 00:00:00"
    updated_record["is_current"] = "T"
    return updated_record


def convert_to_tuples(df):
    values = df.to_records(index=False)
    return values


# Connect Yugabyte DB
db_url = os.environ["YDB_URL"]
engine = create_engine(db_url)
conn = engine.connect()


def full_load(values):
    sql_query2 = f"""INSERT INTO stock_dim (stock_id, company, category, price, start_date, end_date, is_current)
                    VALUES {','.join(map(str, values))};"""
    conn.execute(text(sql_query2))
    conn.commit()
    conn.close()


def incremental_load(values):
    sql_query = f"""INSERT INTO stock_dim (stock_id, company, category, price, start_date, end_date, is_current)
                VALUES {','.join(map(str, values))}
                ON CONFLICT (stock_id, end_date) DO
                UPDATE
                SET
                    end_date = EXCLUDED.start_date - interval '1 second',
                    is_current = 'F'
                WHERE
                    stock_dim.sk_stock_id = (
                SELECT
                    MAX(sk_stock_id)
                FROM
                    stock_dim
                WHERE stock_id = EXCLUDED.stock_id);"""
    sql_query2 = f"""INSERT INTO stock_dim (stock_id, company, category, price, start_date, end_date, is_current)
                    VALUES {','.join(map(str, values))};"""
    conn.execute(text(sql_query))
    conn.execute(text(sql_query2))
    conn.commit()
    conn.close()


if __name__ == "__main__":
    if conn.execute(text("SELECT * FROM stock_dim")).fetchall() == []:
        n_1_date, n_2_date = date_substraction(1, 2)
        csv_1, csv_2 = down_file(n_1_date, n_2_date)
        snap1_df, snap2_df = convert_to_df(csv_1, csv_2)
        full_load(convert_to_tuples(find_updated_records(snap1_df, snap2_df)))
    else:
        n_1_date, n_2_date = date_substraction(1, 2)
        csv_1, csv_2 = down_file(n_1_date, n_2_date)
        snap1_df, snap2_df = convert_to_df(csv_1, csv_2)
        incremental_load(convert_to_tuples(find_updated_records(snap1_df, snap2_df)))
