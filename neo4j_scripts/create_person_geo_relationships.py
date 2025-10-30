import pandas as pd
import os
import sys
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Neo4j connection settings
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Get the directory of the script to ensure proper file path resolution
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE_PATH = os.path.join(SCRIPT_DIR, "edges_person_geo.csv")

def validate_csv_data(df):
    """Validate the CSV data for required fields and data quality."""
    required_fields = ["UserID", "lives_in_area", "lives_in_municipality"]
    
    # Check for required fields
    missing_fields = [field for field in required_fields if field not in df.columns]
    if missing_fields:
        raise ValueError(f"Missing required fields: {missing_fields}")
    
    # Check for empty UserIDs
    empty_user_ids = df["UserID"].isna()
    if empty_user_ids.any():
        empty_indices = df[empty_user_ids].index.tolist()
        raise ValueError(f"Empty UserIDs found at rows: {empty_indices}")
    
    # Check that each person has exactly one location (either area or municipality, not both)
    both_locations = df[(df["lives_in_area"] != "NA") & (df["lives_in_municipality"] != "NA")]
    if not both_locations.empty:
        raise ValueError(f"Persons with both area and municipality found at rows: {both_locations.index.tolist()}")
    
    # Check that each person has at least one location
    no_locations = df[(df["lives_in_area"] == "NA") & (df["lives_in_municipality"] == "NA")]
    if not no_locations.empty:
        print(f"⚠️  Warning: Persons with no location found at rows: {no_locations.index.tolist()}")
    
    # Count area vs municipality relationships
    area_count = (df["lives_in_area"] != "NA").sum()
    municipality_count = (df["lives_in_municipality"] != "NA").sum()
    
    print(f"CSV validation passed. Found {len(df)} valid person-location relationships:")
    print(f"  - {area_count} persons living in areas")
    print(f"  - {municipality_count} persons living in municipalities")

def convert_value(v):
    """Convert pandas values to appropriate Python types for Neo4j."""
    if pd.isna(v):
        return None  # Will be stored as null in Neo4j
    
    # Handle string "NA" values as null
    if isinstance(v, str) and v.strip().upper() == "NA":
        return None
    
    # Convert UserID to int if it's a numeric string
    if isinstance(v, str) and v.isdigit():
        return int(v)
    
    return v

def test_neo4j_connection(driver):
    """Test the Neo4j connection."""
    try:
        with driver.session() as session:
            result = session.run("RETURN 1 as test")
            result.single()
        print("Neo4j connection successful.")
        return True
    except ServiceUnavailable:
        print("Error: Cannot connect to Neo4j server. Please check if the server is running.")
        return False
    except AuthError:
        print("Error: Authentication failed. Please check your credentials.")
        return False
    except Exception as e:
        print(f"Error connecting to Neo4j: {e}")
        return False

def main():
    """Main function to create person-location relationships in Neo4j."""
    try:
        # Check if CSV file exists
        if not os.path.exists(CSV_FILE_PATH):
            raise FileNotFoundError(f"CSV file not found: {CSV_FILE_PATH}")
        
        print(f"Reading CSV file: {CSV_FILE_PATH}")
        
        # Read CSV
        df = pd.read_csv(CSV_FILE_PATH, na_values=[], keep_default_na=False)
        
        # Validate data
        validate_csv_data(df)
        
        # Connect to Neo4j
        print("Connecting to Neo4j...")
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        
        # Test connection
        if not test_neo4j_connection(driver):
            sys.exit(1)
        
        # Process data
        print("Creating person-location relationships in Neo4j...")
        with driver.session() as session:
            with session.begin_transaction() as tx:
                area_relationships = 0
                municipality_relationships = 0
                
                for index, row in df.iterrows():
                    try:
                        user_id = convert_value(row["UserID"])
                        area_name = convert_value(row["lives_in_area"])
                        municipality_name = convert_value(row["lives_in_municipality"])
                        
                        if area_name is not None:
                            # Create LIVES_IN_AREA relationship
                            query = """
                            MATCH (p:Person {UserID: $user_id})
                            MATCH (a:Area {area_name: $area_name})
                            MERGE (p)-[:LIVES_IN_AREA]->(a)
                            """
                            tx.run(query, user_id=user_id, area_name=area_name)
                            area_relationships += 1
                            
                        elif municipality_name is not None:
                            # Create LIVES_IN_MUNICIPALITY relationship
                            query = """
                            MATCH (p:Person {UserID: $user_id})
                            MATCH (m:Municipality {municipality_name: $municipality_name})
                            MERGE (p)-[:LIVES_IN_MUNICIPALITY]->(m)
                            """
                            tx.run(query, user_id=user_id, municipality_name=municipality_name)
                            municipality_relationships += 1
                        
                        # Progress indicator for large dataset
                        if (area_relationships + municipality_relationships) % 100 == 0:
                            print(f"Processed {area_relationships + municipality_relationships} relationships...")
                        
                    except Exception as e:
                        print(f"Error processing row {index} (UserID: {row.get('UserID', 'Unknown')}): {e}")
                        continue
        
        print(f"Successfully created relationships:")
        print(f"  - {area_relationships} LIVES_IN_AREA relationships")
        print(f"  - {municipality_relationships} LIVES_IN_MUNICIPALITY relationships")
        print(f"  - Total: {area_relationships + municipality_relationships} relationships")
        
    except FileNotFoundError as e:
        print(f"File error: {e}")
        sys.exit(1)
    except ValueError as e:
        print(f"Data validation error: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        # Ensure driver is closed
        try:
            driver.close()
        except:
            pass

if __name__ == "__main__":
    main()
