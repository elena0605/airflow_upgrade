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
CSV_FILE_PATH = os.path.join(SCRIPT_DIR, "municipality_nodes.csv")

def validate_csv_data(df):
    """Validate the CSV data for required fields and data quality."""
    required_fields = ["municipality_name"]
    
    # Check for required fields
    missing_fields = [field for field in required_fields if field not in df.columns]
    if missing_fields:
        raise ValueError(f"Missing required fields: {missing_fields}")
    
    # Check for empty municipality names
    empty_municipality_names = df["municipality_name"].isna() | (df["municipality_name"].str.strip() == "")
    if empty_municipality_names.any():
        empty_indices = df[empty_municipality_names].index.tolist()
        raise ValueError(f"Empty municipality names found at rows: {empty_indices}")
    
    # Validate percentage fields (should be 0-100)
    percentage_fields = [col for col in df.columns if "percentage" in col.lower() or "perc" in col.lower()]
    for field in percentage_fields:
        if field in df.columns:
            invalid_percentages = df[(df[field] < 0) | (df[field] > 100)].index.tolist()
            if invalid_percentages:
                print(f"Warning: Invalid percentage values in {field} at rows: {invalid_percentages}")
    
    print(f"CSV validation passed. Found {len(df)} valid municipality records.")

def convert_value(v):
    """Convert pandas values to appropriate Python types for Neo4j."""
    if pd.isna(v):
        return None  # Will be stored as null in Neo4j
    
    # Handle string "NA" values as string (not null)
    if isinstance(v, str) and v.strip().upper() == "NA":
        return "NA"
    
    # Convert floats that are whole numbers to int (except for specific fields that should remain float)
    float_fields = ["housing_property_value_avg", "avg_educational_level", "income_standardized_household_income_avg"]
    if isinstance(v, float) and v.is_integer():
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
    """Main function to import municipality nodes to Neo4j."""
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
        
        # Query template
        query = """
        MERGE (m:Municipality {municipality_name: $municipality_name})
        SET m += $props
        """
        
        # Process data
        print("Importing municipality data to Neo4j...")
        with driver.session() as session:
            with session.begin_transaction() as tx:
                success_count = 0
                for index, row in df.iterrows():
                    try:
                        row_dict = row.to_dict()
                        
                        # Convert all values properly, skip municipality_name in props
                        props = {k: convert_value(v) for k, v in row_dict.items() if k != "municipality_name"}
                        
                        tx.run(query, municipality_name=row["municipality_name"], props=props)
                        success_count += 1
                        
                    except Exception as e:
                        print(f"Error processing row {index} ({row.get('municipality_name', 'Unknown')}): {e}")
                        continue
        
        print(f"Successfully imported {success_count} out of {len(df)} Municipality nodes to Neo4j!")
        
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
