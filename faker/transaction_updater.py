from sqlalchemy import create_engine, text
import random
import datetime


db_user = "admin"
db_password = "password"
db_host = "my-db-instance.cxjkxlbvg4uh.ap-northeast-1.rds.amazonaws.com"
db_name = "stock_db"
db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:3306/{db_name}"

engine = create_engine(db_url)

# Randomize transaction count ratio to total users
def get_info(transaction_count):
    result_user = conn.execute(text(f'SELECT * FROM stock_db.User')).fetchall()
    transaction_count = transaction_count

    stock_info = conn.execute(text(f"SELECT * FROM stock_db.Stock")).fetchall()
    return result_user, stock_info, transaction_count

def user_stock_matching(transaction_count):
    result_user, stock_info, transaction_count = get_info(transaction_count)
    transaction_list = []
    for i in range(transaction_count):
        random_user_loc = random.randint(0, len(result_user) - 1)
        random_stock_loc = random.randint(0, len(stock_info) - 1)
        random_user = result_user[random_user_loc][0]
        random_stock = stock_info[random_stock_loc][0]
        random_quantity = random.randint(100, 1000000)
        random_transaction_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        transaction_list.append(str((random_user, random_stock, random_quantity, random_transaction_date)))
    return transaction_list

def insert_into_db(transaction_count):
    transaction_query = ','.join(user_stock_matching(transaction_count))
    insert_query = f'''
    INSERT INTO stock_db.Transaction (user_id, stock_id, quantity, transaction_date)
    VALUES {transaction_query}
    '''
    conn.execute(text(insert_query))

def updater(transaction_count):
    insert_into_db(transaction_count)
    conn.commit()
    conn.close()

if __name__ == "__main__":
    with engine.connect() as conn:
        updater(20)
