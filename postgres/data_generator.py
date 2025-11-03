import sys
sys.path.append('/opt')
from config.settings import get_settings
import psycopg2
import random
from datetime import datetime
from faker import Faker


def generate_data():
    settings = get_settings()
    conn = psycopg2.connect(
        dbname=settings.POSTGRES_DB,
        user=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        host=settings.POSTGRES_HOST,
        port=settings.POSTGRES_PORT
    )
    cursor = conn.cursor()

    fake = Faker()

    for _ in range(random.randint(5, 10)):
        cursor.execute("""
            INSERT INTO users (email, registration_date, region)
            VALUES (%s, %s, %s)
            ON CONFLICT (email) DO NOTHING
        """, (fake.email(), fake.date_this_year(), fake.state()))

    cursor.execute("SELECT user_id FROM users")
    user_ids = [row[0] for row in cursor.fetchall()]

    for _ in range(random.randint(10, 20)):
        user_id = random.choice(user_ids)
        cursor.execute("""
            INSERT INTO orders (user_id, order_date, amount, status)
            VALUES (%s, %s, %s, %s)
        """, (user_id, fake.date_this_month(),
              round(random.uniform(20, 500), 2),
              random.choice(['completed', 'pending', 'cancelled'])))

    for _ in range(random.randint(50, 100)):
        user_id = random.choice(user_ids)
        cursor.execute("""
            INSERT INTO user_activity (user_id, activity_type, activity_date, duration_seconds)
            VALUES (%s, %s, %s, %s)
        """, (user_id,
              random.choice(['page_view', 'product_view', 'add_to_cart', 'search']),
              fake.date_time_this_month(),
              random.randint(10, 600)))

    conn.commit()
    cursor.close()
    conn.close()
    print(f"Data generated successfully at {datetime.now()}")


if __name__ == "__main__":
    generate_data()