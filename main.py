import logging
import os.path

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

def main():
    try:
        create_logging()
        logging.info(f"Pipeline Initiated!")
        return extract()
    except RuntimeError as re:
        logging.error(f"ERROR occurred on {__name__} : type of error {re}")
    finally:
        logging.info("Pipeline ended!")

if __name__ == "__main__":
    main()