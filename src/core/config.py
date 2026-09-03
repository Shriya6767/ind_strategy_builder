from src.core.modules import os, load_dotenv, psycopg2
    
load_dotenv()

# SENSEX_SPOT_PATH: str = os.getenv("SENSEX_SPOT_PATH")
# SENSEX_FNO_PATH: str = os.getenv("SENSEX_FNO_PATH")
# SENSEX_PROCESSED_PATH: str = os.getenv("SENSEX_PROCESSED_PATH")
# SENSEX_PROCESSED_OHLC_PATH: str = os.getenv("SENSEX_PROCESSED_OHLC_PATH")
SENSEX_PROCESSED_FULL_PATH: str = os.getenv("SENSEX_PROCESSED_FULL_PATH")

DATABASE_HOST = os.getenv("DATABASE_HOST")
DATABASE_PORT = os.getenv("DATABASE_PORT")
DATABASE_NAME = os.getenv("DATABASE_NAME")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")


class Database:
    @staticmethod
    def get_connection():

        conn = psycopg2.connect(
            host=DATABASE_HOST,
            port=DATABASE_PORT,
            database=DATABASE_NAME,
            user=DATABASE_USER,
            password=DATABASE_PASSWORD
        )

        return conn