from sqlalchemy import create_engine, text
import random


db_user = 'admin'
db_password = 'password'
db_host = ''
db_name = 'stock_db'

db_url = f'mysql+pymysql://{db_user}:{db_password}@127.0.0.1:3306/{db_name}'
engine = create_engine(db_url)
conn = engine.connect()


def update_stock_prices():
    select_stock_query = "SELECT stock_id, price FROM Stock"
    stocks = conn.execute(text(select_stock_query))
    for stock in stocks:
        stock_id = stock[0]
        current_price = stock[1]
        # Generate random chance for the stock to stay the same
        chance_same = random.uniform(0, 1)
        if chance_same < 0.02:  # 2% chance for the stock to stay the same
            new_price = current_price
        else:
            # Generate fluctuation within +/- 30%
            fluctuation = random.uniform(-0.3, 0.3)
            new_price = float(round(float(current_price) * (1 + fluctuation), 2))

        # Update Stock table
        update_stock_query = f"UPDATE Stock SET price = {new_price} WHERE stock_id = '{stock_id}'"
        conn.execute(text(update_stock_query))
    conn.commit()


update_stock_prices()
conn.close()
