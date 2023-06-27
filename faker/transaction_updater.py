from sqlalchemy import create_engine, text
import random
import datetime


db_user = "admin"
db_password = "password"
db_host = "my-db-instance.cxjkxlbvg4uh.ap-northeast-1.rds.amazonaws.com"
db_name = "stock_db"
db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:3306/{db_name}"

engine = create_engine(db_url)
conn = engine.connect()

# Randomize transaction count ratio to total users
transaction_rate = random.uniform(0.5, 4)
result_user = conn.execute(text(f"SELECT * FROM stock_db.User")).fetchall()
transaction_count = round(len(result_user) * transaction_rate)

stock_info = conn.execute(text(f"SELECT * FROM stock_db.Stock")).fetchall()

transaction_list = []
for i in range(transaction_count):
    random_user_loc = random.randint(0, len(result_user) - 1)
    random_stock_loc = random.randint(0, len(stock_info) - 1)
    random_user = result_user[random_user_loc][0]
    random_stock = stock_info[random_stock_loc][0]
    random_quantity = random.randint(100, 1000000)
    random_transaction_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    transaction_list.append(
        str((random_user, random_stock, random_quantity, random_transaction_date))
    )

transaction_query = ",".join(transaction_list)
insert_query = f"""
INSERT INTO stock_db.Transaction (user_id, stock_id, quantity, transaction_date)
VALUES {transaction_query}
"""

conn.execute(text(insert_query))
conn.commit()
conn.close()
