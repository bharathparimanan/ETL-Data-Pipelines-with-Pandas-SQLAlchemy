import os.path
import sys
import logging
import pandas as pd

#Add project path to python root path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),'..')))

from src.etl import etl_parquet, extract

def test_data_pipeline():
    try:
        logging.info(
            f"run etl data pipline"
        )
        extract()
        return None
    except Exception as e:
        logging.error(
            f"ERROR occurred while running data pipeline"
        )
        return e

def test_sql_data():
    try:
        logging.info(
            f"check the loaded dataframe is instance of pandas dataframe"
        )
        result = etl_parquet()
        assert isinstance(result, pd.DataFrame)
        return None
    except Exception as e:
        logging.error(
            f"Unit Testing ERROR : {e}"
        )