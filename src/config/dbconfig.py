from sqlalchemy import create_engine

# Database config
username = "postgres"
password = "8055"
host = "localhost"
port = "5432"
database = "dataground"

# Create Database connection engine with connection string
# Format: postgresql+psycopg2://username:password@host:port/database
engine = create_engine(
    f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}"
)