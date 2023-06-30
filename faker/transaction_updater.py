from sqlalchemy import create_engine, text
import random
import datetime
import os

db_url = os.environ['DB_URL']


def updater(event, context):
    engine = create_engine(db_url)
    with engine.connect() as conn:
        insert_into_db(conn, 5)


# Randomize transaction count ratio to total users
def get_info(conn, transaction_count):
    result_user = conn.execute(text(f'SELECT * FROM stock_db.User')).fetchall()
    stock_info = conn.execute(text(f"SELECT * FROM stock_db.Stock")).fetchall()
    return result_user, stock_info, transaction_count


def user_stock_matching(result_user, stock_info, transaction_count):
    transaction_list = []
    for i in range(transaction_count):
        random_user_loc = random.randint(0, len(result_user) - 1)
        random_stock_loc = random.randint(0, len(stock_info) - 1)
        random_user = result_user[random_user_loc][0]
        random_stock = stock_info[random_stock_loc][0]
        random_quantity = random.randint(1, 10)
        random_transaction_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        transaction_list.append(str((random_user, random_stock, random_quantity, random_transaction_date)))
    return transaction_list


def insert_into_db(conn, transaction_count):
    result_user, stock_info, transaction_count = get_info(conn, transaction_count)
    transaction_query = ','.join(user_stock_matching(result_user, stock_info, transaction_count))
    insert_query = f'''
    INSERT INTO stock_db.Transaction (user_id, stock_id, quantity, transaction_date)
    VALUES {transaction_query}
    '''
    conn.execute(text(insert_query))
    conn.commit()
