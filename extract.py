# this script reads the raw CSV file and loads it into a dataframe
# i'm using pandas because that's what most data tutorials use and it works well

import pandas as pd
import logging
import os

# setting up logging so i can see what's happening when the pipeline runs
# learned about logging from the Python docs, way better than using print everywhere
logging.basicConfig(
    filename='logs/pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def extract_data(filepath):
    # check the file actually exists before trying to open it
    # i got a FileNotFoundError the first time i ran this so i added this check
    if not os.path.exists(filepath):
        print(f"Error: cant find the file at {filepath}")
        logging.error(f"File not found: {filepath}")
        raise FileNotFoundError(f"cant find file at {filepath}")

    print(f"Reading data from {filepath}...")
    logging.info(f"Starting extraction from {filepath}")

    df = pd.read_csv(filepath)

    print(f"Done! Loaded {len(df)} rows and {len(df.columns)} columns")
    logging.info(f"Extracted {len(df)} rows successfully")

    return df


# quick test to see if extraction is working
if __name__ == "__main__":
    df = extract_data("data/fraudTest.csv")
    print(df.head())
    print(df.dtypes)
