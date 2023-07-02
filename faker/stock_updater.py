from sqlalchemy import create_engine, text
import random
import os


db_url = os.environ["DB_URL"]
engine = create_engine(db_url)


def update_stock_prices(event, context):
    with engine.connect() as conn:
        select_stock_query = "SELECT stock_id, price FROM Stock"
        stocks = conn.execute(text(select_stock_query))

        for stock in stocks:
            stock_id, current_price = stock
            new_price = generate_new_price(current_price)

            update_stock_query = (
                f"UPDATE Stock SET price = {new_price} WHERE stock_id = '{stock_id}'"
            )
            conn.execute(text(update_stock_query))

        conn.commit()


def generate_new_price(current_price):
    chance_same = random.uniform(0, 1)

    if chance_same < 0.02:
        new_price = current_price
    else:
        fluctuation = random.uniform(-0.3, 0.3)
        new_price = float(round(float(current_price) * (1 + fluctuation), 2))

    return new_price
