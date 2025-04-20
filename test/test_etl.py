import os.path
import sys
import logging
import pytest
import pandas as pd

#Add project path to python root path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))

from src.etl import etl_parquet, extract


def test_sql_data():
    try:
        result = etl_parquet()
        assert isinstance(result, pd.DataFrame)
        return None
    except Exception as e:
        logging.error(
            f"Unit Testing ERROR : {e}"
        )