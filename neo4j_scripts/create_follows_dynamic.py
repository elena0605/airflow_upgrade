#!/usr/bin/env python3
"""
Dynamically create FOLLOWS relationships from follows.csv using Neo4j Python driver.
This approach doesn't require the CSV to be in Neo4j's import directory.
"""

import pandas as pd
import os
import sys
from neo4j import GraphDatabase
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Get the directory of the script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
FOLLOWS_CSV = os.path.join(SCRIPT_DIR, "csv_files", "follows.csv")

def create_follows_relationships():
    """Read follows.csv and create FOLLOWS relationships dynamically."""
    
    if not NEO4J_PASSWORD:
        print("Error: NEO4J_PASSWORD environment variable not set")
        print("Create a .env file with:")
        print("  NEO4J_URI=bolt://localhost:7687")
        print("  NEO4J_USER=neo4j")
        print("  NEO4J_PASSWORD=your_password")
        sys.exit(1)
    
    if not os.path.exists(FOLLOWS_CSV):
        print(f"Error: CSV file not found: {FOLLOWS_CSV}")
        sys.exit(1)
    
    print(f"Reading {FOLLOWS_CSV}...")
    df = pd.read_csv(FOLLOWS_CSV, na_values=[], keep_default_na=False)
    
    # Filter to only records with influencer matches
    df_with_influencers = df[
        df['Influencer'].notna() & 
        (df['Influencer'].str.strip() != "")
    ]
    
    print(f"Found {len(df_with_influencers)} relationships to create")
    
    # Connect to Neo4j
    print(f"Connecting to Neo4j at {NEO4J_URI}...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    # Cypher query to create relationships
    query = """
    MATCH (p:Person {UserID: $user_id})
    MATCH (i:Influencer {name: $influencer})
    MERGE (p)-[:FOLLOWS]->(i)
    """
    
    created_count = 0
    not_found_count = 0
    error_count = 0
    
    print("Creating FOLLOWS relationships...")
    with driver.session() as session:
        for idx, row in df_with_influencers.iterrows():
            try:
                user_id = int(row["UserID"])
                influencer = str(row["Influencer"]).strip()
                
                # Execute the query - MERGE will create the relationship if it doesn't exist
                # Consume the result to ensure the query executes (no warning about multiple records)
                result = session.run(query, user_id=user_id, influencer=influencer)
                result.consume()
                
                # Check if Person and Influencer exist to determine if relationship was created
                check_query = """
                MATCH (p:Person {UserID: $user_id})
                MATCH (i:Influencer {name: $influencer})
                RETURN count(*) AS count
                """
                check_result = session.run(check_query, user_id=user_id, influencer=influencer)
                check_record = check_result.single()
                
                if check_record and check_record["count"] > 0:
                    created_count += 1
                else:
                    not_found_count += 1
                    if not_found_count <= 10:  # Print first 10 not found
                        print(f"  Warning: Person {user_id} or Influencer '{influencer}' not found")
                
            except Exception as e:
                error_count += 1
                if error_count <= 10:  # Print first 10 errors
                    print(f"  Error processing row {idx}: {e}")
                continue
            
            # Progress indicator
            if (idx + 1) % 1000 == 0:
                print(f"  Processed {idx + 1}/{len(df_with_influencers)} relationships...")
    
    driver.close()
    
    # Verify relationships
    print("\nVerifying relationships...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        result = session.run("MATCH ()-[r:FOLLOWS]->() RETURN count(r) AS total")
        total = result.single()["total"]
    
    driver.close()
    
    print("\n" + "=" * 60)
    print("Summary:")
    print(f"  Relationships created: {created_count}")
    print(f"  Not found (Person/Influencer missing): {not_found_count}")
    print(f"  Errors: {error_count}")
    print(f"  Total FOLLOWS relationships in database: {total}")
    print("=" * 60)

if __name__ == "__main__":
    try:
        create_follows_relationships()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

