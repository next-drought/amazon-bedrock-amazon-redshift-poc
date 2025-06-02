import csv
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def get_redshift_connection():
    """Create and return a Redshift connection"""
    REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
    REDSHIFT_PORT = os.getenv('REDSHIFT_PORT', '5439')
    REDSHIFT_DB = os.getenv('REDSHIFT_DB')
    REDSHIFT_USER = os.getenv('REDSHIFT_USER')
    REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
    return create_engine(
        f"redshift+psycopg2://{REDSHIFT_USER}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}"
    )

def check_redshift_connection(engine):
    """Verify Redshift connection works"""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        print(f"Redshift connection error: {str(e)}")
        return False

def create_artists_table(engine):
    """Create artists table in Redshift"""
    with engine.connect() as conn:
        try:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS moma_artists (
                    artist_id INTEGER PRIMARY KEY,
                    full_name VARCHAR(255),
                    nationality VARCHAR(100),
                    gender VARCHAR(50),
                    birth_year INTEGER,
                    death_year INTEGER
                )
            """))
            print("Created artists table")
            return True
        except Exception as e:
            print(f"Error creating table: {str(e)}")
            return False

def load_artist_data(engine, csv_path):
    """Load artist data from CSV into Redshift"""
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        return False

    with engine.connect() as conn:
        with open(csv_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    death_year = int(row['death_year']) if row['death_year'] else None
                    conn.execute(text("""
                        INSERT INTO moma_artists VALUES (
                            :artist_id, :full_name, :nationality,
                            :gender, :birth_year, :death_year
                        )
                    """), {
                        'artist_id': int(row['artist_id']),
                        'full_name': row['full_name'],
                        'nationality': row['nationality'],
                        'gender': row['gender'],
                        'birth_year': int(row['birth_year']),
                        'death_year': death_year
                    })
                    print(f"Loaded artist {row['artist_id']}: {row['full_name']}")
                except Exception as e:
                    print(f"Error loading artist {row['artist_id']}: {str(e)}")
                    continue
    
    print("Data loading completed")
    return True

if __name__ == '__main__':
    engine = get_redshift_connection()
    if not check_redshift_connection(engine):
        print("Failed to connect to Redshift")
        exit(1)

    print("Creating artists table...")
    if create_artists_table(engine):
        print("Loading artist data...")
        csv_path = 'SampleData/moma_public_artists.csv'
        if load_artist_data(engine, csv_path):
            print("Successfully loaded MoMA artist data into Redshift")