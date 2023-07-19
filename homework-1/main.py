"""Скрипт для заполнения данными таблиц в БД Postgres."""
import csv
import psycopg2


def load_from_csv(path_csv: str, table_name: str):
    with open(path_csv, 'r') as f:
        csv_reader = csv.reader(f)

        insert_query = f"INSERT INTO {table_name} VALUES ({','.join(['%s'] * len(next(csv_reader)))})"

        for row in csv_reader:
            cur.execute(insert_query, tuple(row))


path_customers_csv = 'north_data/customers_data.csv'
path_employees_csv = 'north_data/employees_data.csv'
path_orders_csv = 'north_data/orders_data.csv'

conn = psycopg2.connect(
    host="localhost",
    database="north",
    user="postgres",
    password="b6gFXb"
)
try:
    with conn:
        with conn.cursor() as cur:

            load_from_csv(path_customers_csv, "customers")
            load_from_csv(path_employees_csv, "employees")
            load_from_csv(path_orders_csv, "orders")

finally:
    conn.close()
