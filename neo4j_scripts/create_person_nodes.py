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
CSV_FILE_PATH = os.path.join(SCRIPT_DIR, "person_nodes.csv")

def validate_csv_data(df):
    """Validate the CSV data for required fields and data quality."""
    required_fields = ["UserID"]
    
    # Check for required fields
    missing_fields = [field for field in required_fields if field not in df.columns]
    if missing_fields:
        raise ValueError(f"Missing required fields: {missing_fields}")
    
    # Check for empty UserIDs
    empty_user_ids = df["UserID"].isna()
    if empty_user_ids.any():
        empty_indices = df[empty_user_ids].index.tolist()
        raise ValueError(f"Empty UserIDs found at rows: {empty_indices}")
    
    # Check for duplicate UserIDs
    duplicate_user_ids = df["UserID"].duplicated()
    if duplicate_user_ids.any():
        duplicate_ids = df[duplicate_user_ids]["UserID"].tolist()
        print(f"Warning: Duplicate UserIDs found: {duplicate_ids}")
    
    print(f"CSV validation passed. Found {len(df)} valid person records.")

def convert_value(v):
    """Convert pandas values to appropriate Python types for Neo4j."""
    if pd.isna(v):
        return None  # Will be stored as null in Neo4j
    
    # Handle string "NA" values as string (not null)
    if isinstance(v, str) and v.strip().upper() == "NA":
        return "NA"
    
    # Convert floats that are whole numbers to int (except for specific fields that should remain float)
    float_fields = ["inf_category_content_creator___influencer", "inf_category_brand___media_channel", 
                   "inf_category_music_artist", "inf_category_athlete___influencer", 
                   "inf_theme_lifestyle___personal", "inf_theme_comedy___entertainment",
                   "inf_theme_sports___football", "inf_theme_fashion___beauty",
                   "inf_theme_music___performance", "inf_theme_gaming", "inf_theme_news___commentary",
                   "inf_theme_entertainment___stunts", "inf_theme_fitness___health", "inf_theme_business",
                   "inf_scope_other", "inf_scope_netherlands", "inf_scope_rotterdam_region"]
    
    # Convert UserID to int if it's a numeric string
    if isinstance(v, str) and v.isdigit():
        return int(v)
    
    # Convert floats that are whole numbers to int (except for percentage fields)
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
    """Main function to import person nodes to Neo4j."""
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
        MERGE (p:Person {UserID: $UserID})
        SET p += $props
        """
        
        # Define social media account columns to check
        social_media_columns = [
            "tiktok_account1", "tiktok_account2", "tiktok_account3",
            "youtube_account1", "youtube_account2", "youtube_account3"
        ]
        
        # Process data
        print("Importing person data to Neo4j...")
        skipped_count = 0
        with driver.session() as session:
            with session.begin_transaction() as tx:
                success_count = 0
                for index, row in df.iterrows():
                    try:
                        # Check if all social media account columns are "NA" or missing
                        all_accounts_na = True
                        for col in social_media_columns:
                            value = row.get(col, "NA")
                            # Handle both "NA" strings and actual NaN values
                            # If value is not NaN and not "NA", then we have at least one account
                            if not pd.isna(value) and str(value).strip().upper() != "NA":
                                all_accounts_na = False
                                break
                        
                        # Skip this person if all social media accounts are "NA"
                        if all_accounts_na:
                            skipped_count += 1
                            continue
                        
                        row_dict = row.to_dict()
                        
                        # Convert all values properly, skip UserID in props
                        props = {k: convert_value(v) for k, v in row_dict.items() if k != "UserID"}
                        
                        tx.run(query, UserID=row["UserID"], props=props)
                        success_count += 1
                        
                        # Progress indicator for large dataset
                        if success_count % 100 == 0:
                            print(f"Processed {success_count} persons... (skipped {skipped_count} with no social media accounts)")
                        
                    except Exception as e:
                        print(f"Error processing row {index} (UserID: {row.get('UserID', 'Unknown')}): {e}")
                        continue
        
        print(f"Successfully imported {success_count} Person nodes to Neo4j!")
        print(f"Skipped {skipped_count} persons with no social media accounts (all 6 account columns were 'NA').")
        print(f"Total records processed: {success_count + skipped_count} out of {len(df)}")
        
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
