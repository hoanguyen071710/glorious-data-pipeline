from sqlalchemy import create_engine, text
from faker import Faker
import random

db_user = "admin"
db_password = "password"
db_host = "my-db-instance.cxjkxlbvg4uh.ap-northeast-1.rds.amazonaws.com"
db_name = "stock_db"


db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:3306/{db_name}"

engine = create_engine(db_url)
conn = engine.connect()

result = conn.execute(text("select * from stock_db.User")).fetchall()

# Get the number of users that update email
update_rate = random.uniform(0, 0.2)
update_count = round(update_rate * len(result))
# Get update user data
update_user = conn.execute(
    text(f"select * from stock_db.User order by rand() limit {update_count}")
).fetchall()
# Update these users info

fake = Faker()
for usr in update_user:
    user_id = usr[0]
    new_email = fake.email()
    update_query = (
        f"UPDATE stock_db.User SET email = '{new_email}' WHERE user_id = {user_id}"
    )
    conn.execute(text(update_query))

# Create new users
new_user_rate = random.uniform(0, 0.6)
new_user_count = round(new_user_rate * len(result))
new_user_list = []
for new_usr in range(new_user_count):
    user_name = fake.name()
    user_email = fake.email()
    new_user_list.append(str((user_name, user_email)))
# Create query string to insert new users
insert_val = ",".join(new_user_list)
insert_query = f"""
INSERT INTO stock_db.User (name, email) 
VALUES {insert_val}
"""
conn.execute(text(insert_query))

conn.commit()
conn.close()
