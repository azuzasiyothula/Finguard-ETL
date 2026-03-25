
"""
writing tests for my own project felt weird at first but it actually helped me
catch a few bugs i wouldnt have noticed otherwise
i used pytest because its simple
"""

from validate import validate_data
from transform import transform_data
import pytest
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..')))


# some sample data i can reuse across tests
# keeping it small so tests run fast
def get_sample_df():
    return pd.DataFrame({
        'trans_date_trans_time': [
            '2020-01-01 12:00:00',
            '2020-01-02 13:00:00',
            '2020-01-02 13:00:00'  # this is a duplicate of the row above
        ],
        'merchant': ['Shop A', 'Shop B', 'Shop B'],
        'category': ['grocery', 'food', 'food'],
        'amt': [50.00, 1500.00, 1500.00],
        'is_fraud': [0, 1, 1],
        'high_value_flag': [None, None, None]
    })


# --- transform tests ---

def test_duplicates_removed():
    # the sample has 3 rows but one is a duplicate so result should be 2
    df = get_sample_df()
    result = transform_data(df)
    assert len(result) == 2


def test_high_value_flag_applied():
    # amounts over 1000 should get the HIGH flag
    df = get_sample_df()
    result = transform_data(df)
    high_rows = result[result['amt'] > 1000]
    assert all(high_rows['high_value_flag'] == 'HIGH')


def test_normal_flag_applied():
    # amounts under 1000 should get NORMAL
    df = get_sample_df()
    result = transform_data(df)
    normal_rows = result[result['amt'] <= 1000]
    assert all(normal_rows['high_value_flag'] == 'NORMAL')


def test_no_negative_amounts():
    df = get_sample_df()
    result = transform_data(df)
    assert all(result['amt'] > 0)


def test_output_columns_are_correct():
    # make sure the transform didnt drop or rename something it shouldnt have
    df = get_sample_df()
    result = transform_data(df)
    expected = [
        'trans_date_trans_time', 'merchant', 'category',
        'amt', 'is_fraud', 'high_value_flag'
    ]
    assert list(result.columns) == expected


# --- validation tests ---

def test_clean_data_passes_validation():
    # this should pass, everything is correct
    df = pd.DataFrame({
        'trans_date_trans_time': ['2020-01-01 12:00:00'],
        'merchant': ['Shop A'],
        'category': ['grocery'],
        'amt': [50.00],
        'is_fraud': [0],
        'high_value_flag': ['NORMAL']
    })
    assert validate_data(df) == True


def test_negative_amount_fails_validation():
    # negative amount should get caught by the validator
    df = pd.DataFrame({
        'trans_date_trans_time': ['2020-01-01 12:00:00'],
        'merchant': ['Shop A'],
        'category': ['grocery'],
        'amt': [-50.00],
        'is_fraud': [0],
        'high_value_flag': ['NORMAL']
    })
    assert validate_data(df) == False


def test_bad_fraud_flag_fails_validation():
    # fraud flag should only be 0 or 1, anything else should fail
    df = pd.DataFrame({
        'trans_date_trans_time': ['2020-01-01 12:00:00'],
        'merchant': ['Shop A'],
        'category': ['grocery'],
        'amt': [50.00],
        'is_fraud': [5],  # wrong value
        'high_value_flag': ['NORMAL']
    })
    assert validate_data(df) == False
