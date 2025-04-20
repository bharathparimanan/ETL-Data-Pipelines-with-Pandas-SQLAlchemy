import os
import logging
from importlib.metadata import metadata

import pandas as pd
from sqlalchemy import inspect, MetaData, Table, Column, Integer, String, Date, Float

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
        #Except bad lines -- create error while extraction
        df = pd.read_csv(filepath,delimiter=',',on_bad_lines='skip')

        logging.info(f"Shape of the Dataset/DataFrame {df.shape}")
        logging.info(f"Columns of the Dataset/DataFrame {df.columns}")

        return transform(df)
    except Exception as e:
        logging.error(
            f"extraction ERROR occurred in {__name__} : type of error {e}")
        return e

def transform(dataframe):
    """
    pre-processing dataframe before load it into the database
    :param dataframe: from the extract function
    :return: calling load function
    """
    try:
        logging.info(f"transform dataframe initiated!")

        logging.info(f"Pipeline checkpoint for extracted dataframe "
                     f"\nBefore transformation: {dataframe.head(3)}")
        logging.info(f"Datatypes of the Dataframe properties"
                     f"\nBefore transformation : {dataframe.dtypes}")

        #Drop NaN values where the column containing string values
        dataframe = dataframe.dropna(
            subset=['bookID','title','authors','isbn','isbn13','language_code','publication_date','publisher'],
            inplace=False
        )

        int_columns = dataframe.select_dtypes(include='number').columns
        #Fill with 0 where NaN values present
        dataframe[int_columns] = dataframe[int_columns].fillna(0,inplace=False)

        #Rename column name
        dataframe.rename(columns={'  num_pages':'num_pages'},inplace=True)
        #Alter datatype
        dataframe['isbn13'] = dataframe['isbn13'].astype(str)
        #Alter Date -- coerce : If you can't convert this string to a date
        dataframe['publication_date'] = pd.to_datetime(
            dataframe['publication_date'],format='%m/%d/%Y',errors='coerce'
        ).dt.date

        logging.info(f"Pipeline check point transformed data"
                     f"\nAfter transformation : {dataframe.head(3)}")
        logging.info(f"Datatypes of the Dataframe properties"
                     f"\nAfter transformation : {dataframe.dtypes}")

        return load(dataframe)
    except Exception as e:
        logging.error(
            f"transformation ERROR occurred in {__name__} : type of error {e}")
        return e

def load(dataframe):
    try:
        logging.info(f"Load dataframe to SQL")

        #Inspect if the table exist
        Inspector = inspect(engine)
        if 'books' in Inspector.get_table_names():
            logging.info(f"books table present in {Inspector.get_table_names()}")
            #Table exist then replace data
            dataframe.to_sql(
                name="books", #Table name
                con=engine, #Database connection engine
                if_exists='replace', #Replace with new data
                index=False #Avoid writing index name as columns
            )
        else:
            #Table not exist then create a new tabel
            logging.info("create a new table books")

        return None
    except Exception as e:
        logging.error(
            f"loading ERROR occurred in {__name__} : type of error {e}"
        )
        return e