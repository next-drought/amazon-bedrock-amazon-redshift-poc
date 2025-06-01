import os
from amazon_redshift_bedrock_query import get_redshift_uri
from sqlalchemy import create_engine

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