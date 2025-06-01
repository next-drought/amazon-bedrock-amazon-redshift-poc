import boto3
import csv
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize Redshift Data API client
client = boto3.client('redshift-data')

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
    response = client.execute_statement(
        Database=os.getenv('redshift_database'),
        ClusterIdentifier=os.getenv('redshift_cluster'),
        DbUser=os.getenv('redshift_username'),
        Sql=sql
    )
    return response

def load_artist_data(csv_path):
    """Load artist data from CSV into Redshift"""
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
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
                Database=os.getenv('redshift_database'),
                ClusterIdentifier=os.getenv('redshift_cluster'),
                DbUser=os.getenv('redshift_username'),
                Sql=sql
            )
    return True

if __name__ == '__main__':
    # Create table
    create_artists_table()
    
    # Load data from CSV
    csv_path = 'SampleData/moma_public_artists.csv'
    load_artist_data(csv_path)
    
    print("Successfully loaded MoMA artist data into Redshift")