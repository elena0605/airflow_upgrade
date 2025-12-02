#!/usr/bin/env python3
"""
Remove duplicate FOLLOWS relationships.
Keeps one relationship per Person-Influencer pair and deletes the rest.
"""

from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

def remove_duplicate_follows():
    """Remove duplicate FOLLOWS relationships."""
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    with driver.session() as session:
        # Check how many duplicates exist
        check_query = """
        MATCH (p:Person)-[r:FOLLOWS]->(i:Influencer)
        WITH p, i, collect(r) AS relationships
        WHERE size(relationships) > 1
        WITH relationships[1..] AS duplicates
        UNWIND duplicates AS duplicate_rel
        RETURN count(duplicate_rel) AS will_be_deleted
        """
        result = session.run(check_query)
        record = result.single()
        will_be_deleted = record["will_be_deleted"] if record else 0
        
        print(f"Found {will_be_deleted} duplicate relationships to delete")
        
        if will_be_deleted == 0:
            print("No duplicate relationships found. Nothing to delete.")
            driver.close()
            return
        
        # Remove duplicates
        delete_query = """
        MATCH (p:Person)-[r:FOLLOWS]->(i:Influencer)
        WITH p, i, collect(r) AS relationships
        WHERE size(relationships) > 1
        UNWIND relationships[1..] AS duplicate_rel
        DELETE duplicate_rel
        RETURN count(duplicate_rel) AS deleted_count
        """
        
        with session.begin_transaction() as tx:
            result = tx.run(delete_query)
            record = result.single()
            deleted_count = record["deleted_count"] if record else 0
            print(f"Deleted {deleted_count} duplicate relationships")
        
        # Verify final count
        verify_query = "MATCH ()-[r:FOLLOWS]->() RETURN count(r) AS total"
        result = session.run(verify_query)
        total = result.single()["total"]
        print(f"Final total FOLLOWS relationships: {total}")
    
    driver.close()

if __name__ == "__main__":
    remove_duplicate_follows()

