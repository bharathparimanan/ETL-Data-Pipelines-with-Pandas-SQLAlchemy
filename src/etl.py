import os
import logging
import pandas as pd

from .config.dbconfig import engine

def extract():
    """
    extract data from the .csv file and pass to transform function with extracted dataframe
    :return: calling transform function with the extracted dataframe
    """
    try:
        logging.info(f"extract from .csv files initiated!")

        filepath = os.path.abspath(
            os.path.join(os.path.dirname(__file__),"../data/raw/books.csv"))
        #Except bad lines
        df = pd.read_csv(filepath,delimiter=',',on_bad_lines='skip')

        logging.info(f"Shape of the Dataset/DataFrame {df.shape}")
        logging.info(f"Columns of the Dataset/DataFrame {df.columns}")

        return df
    except Exception as e:
        logging.error(
            f"extraction ERROR occurred in {__name__} : type of error {e}")
        return e
