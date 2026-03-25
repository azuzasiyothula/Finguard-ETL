"""
this is the main script that runs everything in order
extract -> transform -> validate -> load
the key thing i wanted to get right here is that if the data fails validation
the pipeline stops and doesnt load anything
i learned from my QA, that catching problems early is way better than fixing them later
"""

import logging
from extract import extract_data
from transform import transform_data
from validate import validate_data
from load import load_data, query_summary

logging.basicConfig(
    filename='logs/pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def run_pipeline():
    print("=" * 50)
    print("  FinGuard ETL Pipeline")
    print("=" * 50)

    # step 1: pull in the raw data
    print("\nStep 1: Extracting data...")
    raw_df = extract_data("data/fraudTest.csv")

    # step 2: clean it up
    print("\nStep 2: Transforming data...")
    clean_df = transform_data(raw_df)

    # step 3: check the data is actually good before saving it
    # this is my favourite part, borrowed the idea from writing test cases
    print("\nStep 3: Validating data quality...")
    is_valid = validate_data(clean_df)

    # if validation fails, stop here, dont load bad data
    if not is_valid:
        print("\nPipeline stopped - data didnt pass the quality checks")
        print("Check logs/pipeline.log to see what failed")
        logging.critical("Pipeline stopped - validation failed")
        return

    # step 4: save the clean data to the database
    print("\nStep 4: Loading clean data to database...")
    load_data(clean_df)
    query_summary()

    print("\n" + "=" * 50)
    print("  Pipeline finished successfully!")
    print("=" * 50)
    logging.info("Pipeline completed")


if __name__ == "__main__":
    run_pipeline()
