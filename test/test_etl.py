import logging
import pytest
import pandas as pd

from src.etl import etl_parquet


def test_sql_data():
    try:
        result = etl_parquet()
        print(result.head(3))
        return None
    except Exception as e:
        logging.error(
            f"Unit Testing ERROR : {e}"
        )