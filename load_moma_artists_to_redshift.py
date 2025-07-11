import csv
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

def get_redshift_connection():
    """Create and return a Redshift connection with autocommit"""
    REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
    REDSHIFT_PORT = os.getenv('REDSHIFT_PORT', '5439')
    REDSHIFT_DB = os.getenv('REDSHIFT_DB')
    REDSHIFT_USER = os.getenv('REDSHIFT_USER')
    REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
    
    # Create engine with autocommit isolation level
    engine = create_engine(
        f"redshift+psycopg2://{REDSHIFT_USER}:{REDSHIFT_PASSWORD}@{REDSHIFT_HOST}:{REDSHIFT_PORT}/{REDSHIFT_DB}",
        isolation_level="AUTOCOMMIT"
    )
    return engine

def check_redshift_connection(engine):
    """Verify Redshift connection works"""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        print(f"Redshift connection error: {str(e)}")
        return False

def inspect_csv_structure(csv_path):
    """Inspect CSV file structure and return column mappings"""
    if not os.path.exists(csv_path):
        print(f"Error: CSV file not found at {csv_path}")
        return None
    
    with open(csv_path, 'r', encoding='utf-8-sig') as f:  # utf-8-sig automatically handles BOM
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        
        print(f"CSV headers found: {headers}")
        
        # Try to read first row to see sample data
        try:
            first_row = next(reader)
            print(f"Sample row: {dict(list(first_row.items())[:5])}...")  # Show first 5 fields
        except StopIteration:
            print("CSV file appears to be empty")
            return None
    
    # Create column mapping based on common variations
    column_mapping = {}
    
    for header in headers:
        # Remove BOM and clean header
        header_clean = header.replace('\ufeff', '').lower().strip()
        
        # Map various possible column names to our schema
        if header_clean in ['artist_id', 'artistid', 'id', 'constituentid']:
            column_mapping['artist_id'] = header
        elif header_clean in ['full_name', 'name', 'artist_name', 'display_name', 'displayname']:
            column_mapping['full_name'] = header
        elif header_clean in ['nationality', 'nation']:
            column_mapping['nationality'] = header
        elif header_clean in ['gender', 'sex']:
            column_mapping['gender'] = header
        elif header_clean in ['birth_year', 'birthyear', 'birth', 'born', 'beginyear']:
            column_mapping['birth_year'] = header
        elif header_clean in ['death_year', 'deathyear', 'death', 'died', 'endyear']:
            column_mapping['death_year'] = header
    
    print(f"Column mapping: {column_mapping}")
    return column_mapping

def safe_int_convert(value):
    """Safely convert value to int, handling empty strings and None"""
    if value is None or value == '' or str(value).strip() == '':
        return None
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return None

def safe_str_convert(value):
    """Safely convert value to string, handling None"""
    if value is None or value == '':
        return None
    return str(value).strip() if str(value).strip() else None

def create_artists_table(engine):
    """Create artists table in Redshift"""
    try:
        with engine.connect() as conn:
            # Drop table if exists
            conn.execute(text("DROP TABLE IF EXISTS artists"))
            
            # Create table
            conn.execute(text("""
                CREATE TABLE artists (
                    artist_id INTEGER NOT NULL,
                    full_name VARCHAR(200),
                    nationality VARCHAR(50),
                    gender VARCHAR(25),
                    birth_year INTEGER,
                    death_year INTEGER,
                    CONSTRAINT artists_pk PRIMARY KEY (artist_id)
                )
            """))
        print("Created artists table")
        return True
    except Exception as e:
        print(f"Error creating table: {str(e)}")
        return False

def load_artist_data(engine, csv_path):
    """Load artist data from CSV into Redshift"""
    column_mapping = inspect_csv_structure(csv_path)
    if not column_mapping:
        return False
    
    # Check if we have the essential columns
    if 'artist_id' not in column_mapping:
        print("Error: No artist_id column found in CSV")
        print("Available mappings:", column_mapping)
        return False
    
    success_count = 0
    error_count = 0
    
    try:
        with engine.connect() as conn:
            with open(csv_path, 'r', encoding='utf-8-sig') as f:  # utf-8-sig handles BOM
                reader = csv.DictReader(f)
                
                for row_num, row in enumerate(reader, 1):
                    try:
                        # Extract values using our column mapping
                        artist_id = safe_int_convert(row.get(column_mapping['artist_id']))
                        if artist_id is None:
                            if row_num <= 5:  # Only show first few invalid IDs
                                print(f"Row {row_num}: Skipping row with invalid artist_id: '{row.get(column_mapping['artist_id'])}'")
                            continue
                        
                        full_name = safe_str_convert(row.get(column_mapping.get('full_name')))
                        nationality = safe_str_convert(row.get(column_mapping.get('nationality')))
                        gender = safe_str_convert(row.get(column_mapping.get('gender')))
                        birth_year = safe_int_convert(row.get(column_mapping.get('birth_year')))
                        death_year = safe_int_convert(row.get(column_mapping.get('death_year')))
                        
                        # Insert into database
                        conn.execute(text("""
                            INSERT INTO artists (artist_id, full_name, nationality, gender, birth_year, death_year)
                            VALUES (:artist_id, :full_name, :nationality, :gender, :birth_year, :death_year)
                        """), {
                            'artist_id': artist_id,
                            'full_name': full_name,
                            'nationality': nationality,
                            'gender': gender,
                            'birth_year': birth_year,
                            'death_year': death_year
                        })
                        
                        success_count += 1
                        if success_count % 500 == 0:
                            print(f"Loaded {success_count} artists...")
                            
                    except Exception as e:
                        error_count += 1
                        if error_count <= 5:  # Only show first few errors
                            artist_id_str = row.get(column_mapping.get('artist_id', ''), 'unknown')
                            print(f"Row {row_num}: Error loading artist {artist_id_str}: {str(e)}")
                        if error_count > 100:  # Stop if too many errors
                            print("Too many errors, stopping...")
                            break
                        continue
        
        print(f"Data loading completed. Success: {success_count}, Errors: {error_count}")
        return success_count > 0
        
    except Exception as e:
        print(f"Loading failed: {str(e)}")
        return False

def verify_data_load(engine):
    """Verify the data was loaded correctly"""
    with engine.connect() as conn:
        try:
            # Count total rows
            result = conn.execute(text("SELECT COUNT(*) FROM artists"))
            count = result.fetchone()[0]
            print(f"Total artists loaded: {count}")
            
            # Show a few sample rows
            result = conn.execute(text("SELECT * FROM artists LIMIT 5"))
            rows = result.fetchall()
            print("\nSample data:")
            for row in rows:
                print(f"  ID: {row[0]}, Name: {row[1]}, Nationality: {row[2]}, Gender: {row[3]}, Born: {row[4]}, Died: {row[5]}")
                
            return True
        except Exception as e:
            print(f"Error verifying data: {str(e)}")
            return False

if __name__ == '__main__':
    print("Connecting to Redshift...")
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
            verify_data_load(engine)
        else:
            print("Failed to load artist data")
    else:
        print("Failed to create artists table")