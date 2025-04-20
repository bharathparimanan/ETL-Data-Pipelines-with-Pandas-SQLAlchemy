import os
import logging
from importlib.metadata import metadata

import pandas as pd
from sqlalchemy import inspect, MetaData, Table, Column, Integer, String, Date, Float

from .config.dbconfig import engine

dirpath = os.path.abspath(
            os.path.join(os.path.dirname(__file__),"../data"))

def extract():
    """
    extract data from the .csv file and pass to transform function with extracted dataframe
    :return: calling transform function with the extracted dataframe
    """
    try:
        logging.info(f"extract from .csv files initiated!")
        filepath = os.path.join(dirpath, 'raw/books.csv')
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
            #Create new table
            metadata = MetaData()
            books_table = Table(
                'books',
                metadata,
                Column('bookID', Integer, primary_key=True),
                Column('title', String),
                Column('authors', String),
                Column('average_rating', Float),
                Column('isbn', String),
                Column('isbn13', String),
                Column('language_code', String),
                Column('num_pages', Integer),
                Column('ratings_count', Integer),
                Column('text_reviews_count', Integer),
                Column('publication_date', Date),
                Column('publisher', String)
            )
            metadata.create_all(engine)

            #Load data from dataframe
            dataframe.to_sql(
                name="books", #Table name
                con=engine, #Database connection engine
                if_exists='append',
                index=False, #Avoid index as new column
            )
        return read_sql()
    except Exception as e:
        logging.error(
            f"loading ERROR occurred in {__name__} : type of error {e}"
        )
        return e

def read_sql():
    try:
        #read data from database
        dataframe = pd.read_sql('select * from books',con=engine)

        logging.info(
            f"Pipline checkpoints data reed from database {dataframe.head(3)}"
            f"\nDataframe shape : {dataframe.shape}"
            f"\nDataframe column datatypes : {dataframe.dtypes}"
        )

        #Create a parquet file with dataframe
        filepath = os.path.join(dirpath,'processed/books.parquet')
        dataframe.to_parquet(filepath, engine='pyarrow')

        logging.info(
            f"new parquet file generated with the extracted data from database"
        )

        return None
    except Exception as e:
        logging.error(
            f"Database ERROR occurred in {__name__} : type of the error {e}"
        )
        return e