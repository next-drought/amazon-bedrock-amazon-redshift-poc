import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_redshift_uri():
    REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
    REDSHIFT_PORT = os.getenv('REDSHIFT_PORT', '5439')  # Default port
    REDSHIFT_DB = os.getenv('REDSHIFT_DB')
    REDSHIFT_USER = os.getenv('REDSHIFT_USER')
    REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
    return f"redshift+psycopg2://{REDSHIFT_USER}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}"

# Test Redshift connection
def test_connection():
    try:
        redshift_uri = get_redshift_uri()
        engine = create_engine(redshift_uri)
        with engine.connect() as conn:
            print("Successfully connected to Redshift!")
            # Test a simple query
            result = conn.execute("SELECT 1")
            print("Test query result:", result.fetchone())
        return True
    except Exception as e:
        print(f"Connection failed: {str(e)}")
        return False

if __name__ == "__main__":
    test_connection()