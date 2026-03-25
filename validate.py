"""
this is the part i'm most proud of honestly
the idea came from my QA internship, we never let code go to production without tests
so i thought why not do the same thing for data?
i'm using Pandera for this, i originally tried Great Expectations but it kept breaking with Python 3.14 (pydantic v1 compatibility issue). 
after some googling i found Pandera which does the same thing and actually has cleaner syntax
"""

import pandas as pd
import pandera.pandas as pa
from pandera.typing import Series
import logging

logging.basicConfig(
    filename='logs/pipeline.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

"""
this class defines what "good data" looks like
if the data doesn't match these rules it fails the check and the pipeline stops
i got this idea from how we write test cases in QA, define what pass looks like first
"""


class TransactionSchema(pa.DataFrameModel):

    # the transaction timestamp must exist
    trans_date_trans_time: Series[str]

    # merchant cant be empty,  every transaction needs a merchant
    merchant: Series[str] = pa.Field(nullable=False)

    # category cant be empty either
    category: Series[str] = pa.Field(nullable=False)

    # amount must be a real positive number
    # i set the max at 50000, anything above that seems suspicious anyway
    amt: Series[float] = pa.Field(
        gt=0,
        le=50000,
        nullable=False
    )

    # fraud column should only ever be 0 or 1, its a binary flag
    # found some weird values in early testing which is why i added this
    is_fraud: Series[int] = pa.Field(
        isin=[0, 1],
        nullable=False
    )

    # the flag i created in transform, should only be NORMAL or HIGH
    high_value_flag: Series[str] = pa.Field(
        isin=["NORMAL", "HIGH"],
        nullable=False
    )

    class Config:
        coerce = True   # tries to fix minor type issues automatically
        strict = False  # wont fail if there are extra columns


def validate_data(df):
    print("\nRunning data quality checks...")
    print("-" * 40)
    logging.info("Starting validation")

    try:
        # lazy=True means it checks everything and shows ALL failures at once
        # instead of stopping at the first one, much more useful for debugging
        TransactionSchema.validate(df, lazy=True)

        print("merchant column: OK")
        print("category column: OK")
        print("amount range: OK")
        print("fraud flag values: OK")
        print("high value flag values: OK")
        print("-" * 40)
        print("All checks passed - safe to load the data\n")
        logging.info("Validation passed")
        return True

    except pa.errors.SchemaErrors as err:
        # print out exactly what failed so its easy to debug
        print("Validation FAILED - stopping the pipeline")
        print("\nHere's what went wrong:")
        print(err.failure_cases.to_string())
        print("-" * 40)
        print("Data was NOT loaded. Check the logs for more info\n")
        logging.error(f"Validation failed: {err.failure_cases}")
        return False

    except Exception as e:
        print(f"Something unexpected went wrong: {e}")
        logging.error(f"Unexpected error in validation: {e}")
        return False


if __name__ == "__main__":
    from extract import extract_data
    from transform import transform_data
    raw = extract_data("data/fraudTest.csv")
    clean = transform_data(raw)
    result = validate_data(clean)
    print(f"Result: {'PASSED' if result else 'FAILED'}")
