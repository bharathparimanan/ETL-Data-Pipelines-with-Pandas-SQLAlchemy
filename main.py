import logging
import os.path

from src.config.dbconfig import engine
from src.etl import extract

def create_logging():
    try:
        log_filepath = os.path.abspath(os.path.join(os.path.dirname(__file__),"logs/pipelines.log"))
        if os.path.exists(log_filepath):
            logging.basicConfig(filename=log_filepath,level=logging.INFO,
                                format="%(asctime)s - %(levelname)s - %(message)s")

            logging.basicConfig(filename=log_filepath, level=logging.ERROR,
                                format="%(asctime)s - %(levelname)s - %(message)s")
        else:
            logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
            logging.warning(f"Log file not found at: {log_filepath}. Logging to console.")
        return None
    except FileNotFoundError as fe:
        return fe

def connect_database():
    try:
        with engine.connect() as conn:
            logging.info(f"Check Database Connection : {conn} : Connected Successfully")

        #Disconnect Connection
        conn.close()

        return None
    except Exception as e:
        print(e)
        logging.error(f"Database connection ERROR occurred in {__name__} : type of error : {e}")
        return e

def main():
    try:
        create_logging()
        connect_database()
        logging.info(f"Pipeline Initiated!")
        return extract()
    except RuntimeError as re:
        logging.error(f"ERROR occurred on {__name__} : type of error {re}")
    finally:
        logging.info("Pipeline ended!")

if __name__ == "__main__":
    main()