"""
final step: takes the clean validated data and saves it to a SQLite database
i chose SQLite because it doesn't need any server setup, just a file
in a real job this would probably be PostgreSQL or something cloud based 
"""

import pandas as pd
import sqlite3
import logging

logging.basicConfig(
    filename='logs/pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def load_data(df, db_path="data/finguard.db"):
    print(f"Saving {len(df)} rows to database...")
    logging.info(f"Loading data into {db_path}")

    conn = sqlite3.connect(db_path)

    # if_exists="replace" means it overwrites the table each run
    # not ideal for production but fine for this project
    df.to_sql("transactions", conn, if_exists="replace", index=False)

    conn.close()
    print(f"Done! Data saved to {db_path}")
    logging.info("Data loaded successfully")


def query_summary(db_path="data/finguard.db"):
    # just a quick check to make sure the data actually landed correctly
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print("\nQuick summary of what got loaded:")
    print("-" * 40)

    cursor.execute("SELECT COUNT(*) FROM transactions")
    print(f"Total rows: {cursor.fetchone()[0]}")

    cursor.execute("SELECT COUNT(*) FROM transactions WHERE is_fraud = 1")
    print(f"Fraudulent transactions: {cursor.fetchone()[0]}")

    cursor.execute(
        "SELECT COUNT(*) FROM transactions WHERE high_value_flag = 'HIGH'")
    print(f"High value transactions: {cursor.fetchone()[0]}")

    cursor.execute("SELECT ROUND(AVG(amt), 2) FROM transactions")
    print(f"Average amount: {cursor.fetchone()[0]}")

    print("-" * 40)

    conn.close()


if __name__ == "__main__":
    from extract import extract_data
    from transform import transform_data
    raw = extract_data("data/fraudTest.csv")
    clean = transform_data(raw)
    load_data(clean)
    query_summary()
