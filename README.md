# ETL Data Pipelines with Pandas & SQLAlchemy

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.x](https://img.shields.io/badge/python-3.x-blue.svg)](https://www.python.org/downloads/)
[![Imports: Pandas, SQLAlchemy, PostgreSQL, Pytest, Logging](https://img.shields.io/badge/imports-Pandas%20%7C%20SQLAlchemy%20%7C%20PostgreSQL%20%7C%20Pytest%20%7C%20Logging-brightgreen)](https://pypi.org/)

## Overview

This repository demonstrates the creation of a robust Extract, Transform, Load (ETL) data pipeline leveraging Pandas and SQLAlchemy. It's designed to ingest raw data from CSV files located in the `data/raw/` directory, process and clean this data using Pandas within the `src/etl.py` module, and then load the transformed data into a PostgreSQL database using SQLAlchemy, with connection details configured in `src/config/dbconfig.py`. Additionally, the pipeline supports extracting data from the PostgreSQL database to generate processed files stored in the `data/processed/` directory. All service logs are captured in `logs/pipelines.log`. The project includes comprehensive unit tests in `test/test.py` (using Pytest) to ensure the pipeline's reliability. `main.py` serves as the entry point for executing the ETL process.

This project aims to streamline data preparation workflows for data scientists, data analysts, and data engineers, providing a well-structured and tested solution for data integration.

## Key Features

* **Data Extraction from Raw CSV:** Reads unprocessed data files from the `data/raw/` directory using Pandas.
* **Data Transformation with Pandas:** Implements data cleaning and transformation logic within the `src/etl.py` module, handling null values and rectifying inconsistencies.
* **Data Loading into PostgreSQL:** Utilizes SQLAlchemy (configured in `src/config/dbconfig.py`) to connect to a PostgreSQL database and load the transformed data.
* **Data Extraction to Processed Files:** Reads data from the PostgreSQL database and generates processed data files stored in the `data/processed/` directory.
* **Centralized ETL Logic:** All ETL functionalities are encapsulated within the `src/etl.py` module for better organization.
* **Database Configuration:** Database connection strings are managed in a dedicated `src/config/dbconfig.py` file.
* **Comprehensive Unit Tests:** Includes unit tests in `test/test.py` using Pytest to ensure the correctness of individual components.
* **Service Logging:** All pipeline activities and potential errors are logged to `logs/pipelines.log` using Python's `logging` module.
* **Clear Entry Point:** The `main.py` script serves as the starting point for running the entire ETL process.

## Technologies Used

* **Python:** The primary programming language.
* **Pandas:** A powerful library for data manipulation and analysis.
* **SQLAlchemy:** A Python SQL toolkit and Object-Relational Mapper (ORM) for database interaction.
* **PostgreSQL:** A robust, open-source relational database system.
* **Pytest:** A popular Python testing framework for writing concise and readable tests.
* **Logging:** Python's built-in module for generating log messages.

## Setup and Installation

1.  **Clone the repository:**
    ```bash
    git clone <repository_url>
    cd ETL-Data-Pipelines-with-Pandas-SQLAlchemy
    ```

2.  **Install required dependencies:**
    It is recommended to create a virtual environment first.
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Linux/macOS
    venv\Scripts\activate  # On Windows
    pip install -r requirements.txt
    ```
    *(Ensure `requirements.txt` includes `pandas`, `sqlalchemy`, `psycopg2-binary` (for PostgreSQL), and `pytest`.)*

3.  **Set up PostgreSQL:**
    * Ensure you have PostgreSQL installed and running.
    * Create a database that will be used by the pipeline.
    * Configure the database connection details (host, port, username, password, database name) within the `src/config/dbconfig.py` file.

4.  **Prepare Raw CSV Data:**
    * Place your unprocessed CSV files in the `data/raw/` directory.

## Usage

1.  **Configure Database Connection:**
    * Modify the database connection parameters in `src/config/dbconfig.py` to match your PostgreSQL setup.

2.  **Run the ETL Pipeline:**
    * Execute the `main.py` script to start the ETL process.
    ```bash
    python main.py
    ```
    * This will trigger the extraction of data from `data/raw/`, the transformations defined in `src/etl.py`, and the loading of data into the PostgreSQL database. It might also include the process of extracting data from the database to generate files in `data/processed/`. Check the `main.py` script for the exact workflow.

3.  **View Logs:**
    * Monitor the progress and any errors by inspecting the `logs/pipelines.log` file.

4.  **Run Tests:**
    * Navigate to the root directory of the repository and run the Pytest test suite.
    ```bash
    pytest
    ```
    * This will execute all the test cases defined in `test/test.py` to verify the functionality of the `src/etl.py` module.

## Project Structure

ETL-Data-Pipelines-with-Pandas-SQLAlchemy/
├── data/
│   ├── raw/
│   │   └── sample_raw_data.csv
│   └── processed/
│       └── processed_data.csv (example output)
├── logs/
│   └── pipelines.log
├── src/
│   ├── config/
│   │   └── dbconfig.py
│   └── etl.py
├── test/
│   └── test.py
├── main.py
├── requirements.txt
├── README.md
└── .gitignore

* `data/raw/`: Contains the original, unprocessed CSV data files.
* `data/processed/`: Stores data files generated after extraction from the database.
* `logs/`: Holds the `pipelines.log` file containing service logs.
* `src/config/`: Contains the `dbconfig.py` file with database connection details.
* `src/etl.py`: Contains all the core ETL logic implemented using Pandas and SQLAlchemy.
* `test/test.py`: Includes unit test cases written using Pytest to test the functions in `src/etl.py`.
* `main.py`: The main script to initiate and run the ETL pipeline.
* `requirements.txt`: Lists the Python packages required for the project.
* `README.md`: This file, providing an overview of the project.
* `.gitignore`: Specifies intentionally untracked files that Git should ignore.

## Contributing

Contributions to this repository are welcome. If you find any issues or have suggestions for improvements, please feel free to open an issue or submit a pull request.

## License

This project is licensed under the [MIT License](LICENSE). See the `LICENSE` file for more details.

## Contact
linkedin.com/in/bharath-parimanan-940698207
