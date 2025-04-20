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

        return dataframe
    except Exception as e:
        logging.error(
            f"transformation ERROR occurred in {__name__} : type of error {e}")
        return e
