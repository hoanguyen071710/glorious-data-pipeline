from sqlalchemy import create_engine, text
from faker import Faker
import random
import os
import datetime


db_url = os.environ["DB_URL"]

engine = create_engine(db_url)


def update_rate(conn):
    result = conn.execute(text("SELECT * FROM stock_db.User")).fetchall()
    update_rate = random.uniform(0, 0.02)
    update_count = round(update_rate * len(result))
    # Get update user data
    update_user = conn.execute(
        text(f"SELECT * FROM stock_db.User ORDER BY RAND() LIMIT {update_count}")
    ).fetchall()
    return update_user


def update_current_user(conn):
    fake = Faker()
    update_user = update_rate(conn)
    for usr in update_user:
        user_id = usr[0]
        new_email = fake.email()
        updated_at = (datetime.datetime.now() + datetime.timedelta(hours=7)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        update_query = f"""
        UPDATE stock_db.User 
        SET email = '{new_email}', updated_at = '{updated_at}', status = 'U' 
        WHERE user_id = {user_id}
        """
        conn.execute(text(update_query))


def create_new_user(new_user_count, conn):
    fake = Faker()
    new_user_count = new_user_count
    new_user_list = []
    for new_usr in range(new_user_count):
        user_name = fake.name()
        user_email = fake.email()
        updated_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        status = "I"
        new_user_list.append(str((user_name, user_email, updated_at, status)))
    # Create query string to insert new users
    insert_val = ",".join(new_user_list)
    insert_query = f"""
    INSERT INTO stock_db.User (name, email, updated_at, status) 
    VALUES {insert_val}
    """
    conn.execute(text(insert_query))
    conn.commit()


def updater(event, context):
    with engine.connect() as conn:
        update_current_user(conn)
        create_new_user(10, conn)
