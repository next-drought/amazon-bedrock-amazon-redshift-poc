import boto3
import csv
import os
import time
from dotenv import load_dotenv

load_dotenv()

# Initialize Redshift Data API client
client = boto3.client('redshift-data', region_name=os.getenv('BEDROCK_REGION'))

def check_redshift_connection():
    """Verify Redshift connection works by executing a simple query"""
    try:
        response = client.execute_statement(
            Database=os.getenv('REDSHIFT_DB'),
            ClusterIdentifier=os.getenv('REDSHIFT_HOST'),
            DbUser=os.getenv('REDSHIFT_USER'),
            Sql="SELECT 1"
        )
        return True
    except Exception as e:
        print(f"Redshift connection error: {str(e)}")
        print("Please verify:")
        print(f"- REDSHIFT_HOST, REDSHIFT_DB, REDSHIFT_USER are set correctly in .env")
        print(f"- IAM role has AmazonRedshiftDataFullAccess permission")
        print(f"- Redshift cluster is running and accessible")
        return False

def create_artists_table():
    """Create artists table in Redshift"""
    sql = """
    CREATE TABLE IF NOT EXISTS moma_artists (
        artist_id INTEGER PRIMARY KEY,
        full_name VARCHAR(255),
        nationality VARCHAR(100),
        gender VARCHAR(50),
        birth_year INTEGER,
        death_year INTEGER
    )
    """
    try:
        response = client.execute_statement(
            Database=os.getenv('REDSHIFT_DB'),
            ClusterIdentifier=os.getenv('REDSHIFT_HOST'),
            DbUser=os.getenv('REDSHIFT_USER'),
            Sql=sql
        )
        print(f"Created artists table with ID: {response['Id']}")
        return response
    except Exception as e:
        print(f"Error creating table: {str(e)}")
        return None

def load_artist_data(csv_path):
    """Load artist data from CSV into Redshift"""
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        return False

    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                death_year = f"'{row['death_year']}'" if row['death_year'] else 'NULL'
                sql = f"""
                INSERT INTO moma_artists VALUES (
                    {row['artist_id']},
                    '{row['full_name'].replace("'", "''")}',
                    '{row['nationality']}',
                    '{row['gender']}',
                    {row['birth_year']},
                    {death_year}
                )
                """
                response = client.execute_statement(
                    Database=os.getenv('REDSHIFT_DB'),
                    ClusterIdentifier=os.getenv('REDSHIFT_HOST'),
                    DbUser=os.getenv('REDSHIFT_USER'),
                    Sql=sql
                )
                print(f"Loaded artist {row['artist_id']}: {row['full_name']}")
            except Exception as e:
                print(f"Error loading artist {row['artist_id']}: {str(e)}")
                continue
    
    print("Data loading completed")
    return True

if __name__ == '__main__':
    if not check_redshift_connection():
        print("Failed to connect to Redshift")
        exit(1)

    print("Creating artists table...")
    create_response = create_artists_table()
    
    if create_response:
        print("Loading artist data...")
        csv_path = 'SampleData/moma_public_artists.csv'
        load_success = load_artist_data(csv_path)
        
        if load_success:
            print("Successfully loaded MoMA artist data into Redshift")