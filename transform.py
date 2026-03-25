# this is where i clean up the raw data before it goes into the database
# there was a lot of messy stuff in the dataset: duplicates, weird column names, nulls etc
# i figured out most of this by just running df.describe() and df.info() and seeing what looked off

import pandas as pd
import logging

logging.basicConfig(
    filename='logs/pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def transform_data(df):
    print("Starting data cleanup...")
    logging.info("Transform step started")

    rows_at_start = len(df)

    # fix the column names, some had spaces and uppercase which was annoying to work with
    df.columns = df.columns.str.lower().str.replace(' ', '_')

    # remove duplicate rows, found quite a few when i first explored the data
    df = df.drop_duplicates()
    duplicates_removed = rows_at_start - len(df)
    print(f"Removed {duplicates_removed} duplicate rows")
    logging.info(f"Removed {duplicates_removed} duplicates")

    # drop rows where the important columns are empty
    # these columns are the ones that actually matter for analysis
    important_columns = [
        'amt', 'trans_date_trans_time', 'merchant', 'category']
    before_null_drop = len(df)
    df = df.dropna(subset=important_columns)
    print(f"Removed {before_null_drop - len(df)} rows with missing values")
    logging.info(f"Dropped {before_null_drop - len(df)} null rows")

    # remove any transactions where the amount is 0 or negative
    # that shouldnt happen with real transaction data
    df = df[df['amt'] > 0]

    # convert the date column to an actual datetime type
    # it was coming in as a string which made it hard to work with
    df['trans_date_trans_time'] = pd.to_datetime(
        df['trans_date_trans_time'], errors='coerce'
    )

    # add a flag for high value transactions (over R1000)
    # this seemed useful for fraud detection purposes
    df['high_value_flag'] = df['amt'].apply(
        lambda x: 'HIGH' if x > 1000 else 'NORMAL'
    )

    # round the amounts to 2 decimal places like actual currency
    df['amt'] = df['amt'].round(2)

    # only keep the columns i actually need
    # there were like 20 columns in the original and most werent useful
    columns_to_keep = [
        'trans_date_trans_time', 'merchant', 'category',
        'amt', 'is_fraud', 'high_value_flag'
    ]
    df = df[columns_to_keep]

    print(f"Cleanup done. {len(df)} rows ready to validate")
    logging.info(f"Transform complete - {len(df)} rows remaining")

    return df


if __name__ == "__main__":
    from extract import extract_data
    raw = extract_data("data/fraudTest.csv")
    clean = transform_data(raw)
    print(clean.head())
    print(clean.dtypes)
