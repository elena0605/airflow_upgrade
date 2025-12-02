#!/usr/bin/env python3
"""
Remove duplicate Person nodes by UserID.
Merges relationships from duplicate nodes to the original node, then deletes duplicates.
"""

from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

def get_all_relationship_types(session):
    """Fetch all relationship types in the database."""
    result = session.run("CALL db.relationshipTypes()")
    return [record["relationshipType"] for record in result]

def remove_duplicate_person_nodes():
    """Remove duplicate Person nodes by UserID."""
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    
    with driver.session() as session:
        # Step 1: Get all relationship types dynamically
        relationship_types = get_all_relationship_types(session)
        print(f"Found relationship types: {relationship_types}")
        
        # Step 2: Find duplicate Person nodes by UserID
        duplicates_query = """
        MATCH (n:Person)
        WHERE n.UserID IS NOT NULL
        WITH n.UserID AS user_id, collect(n) AS nodes, count(*) AS count
        WHERE count > 1
        RETURN user_id, nodes[0] AS original, nodes[1..] AS duplicates
        """
        
        result = session.run(duplicates_query)
        duplicates_list = list(result)
        print(f"Found {len(duplicates_list)} UserIDs with duplicate Person nodes")
        
        if len(duplicates_list) == 0:
            print("No duplicate Person nodes found. Nothing to delete.")
            driver.close()
            return
        
        deleted_count = 0
        processed = 0
        
        # Step 3: Process each duplicate group
        for record in duplicates_list:
            user_id = record["user_id"]
            original_node = record["original"]
            duplicate_nodes = record["duplicates"]
            original_id = original_node.id
            
            print(f"Processing UserID {user_id}: {len(duplicate_nodes)} duplicate(s)")
            
            with session.begin_transaction() as tx:
                for duplicate_node in duplicate_nodes:
                    duplicate_id = duplicate_node.id
                    
                    # Merge properties from duplicate to original (keep original's values, add missing ones)
                    merge_properties_query = """
                    MATCH (original), (duplicate)
                    WHERE id(original) = $orig_id AND id(duplicate) = $dup_id
                    SET original += duplicate
                    """
                    tx.run(merge_properties_query, orig_id=original_id, dup_id=duplicate_id)
                    
                    # Process each relationship type
                    for rel_type in relationship_types:
                        # Move outgoing relationships using MERGE to avoid duplicates
                        outgoing_query = f"""
                        MATCH (duplicate)-[r:{rel_type}]->(other)
                        WHERE id(duplicate) = $dup_id
                        MATCH (original)
                        WHERE id(original) = $orig_id
                        MERGE (original)-[:{rel_type}]->(other)
                        DELETE r
                        """
                        tx.run(outgoing_query, dup_id=duplicate_id, orig_id=original_id)
                        
                        # Move incoming relationships using MERGE to avoid duplicates
                        incoming_query = f"""
                        MATCH (other)-[r:{rel_type}]->(duplicate)
                        WHERE id(duplicate) = $dup_id
                        MATCH (original)
                        WHERE id(original) = $orig_id
                        MERGE (other)-[:{rel_type}]->(original)
                        DELETE r
                        """
                        tx.run(incoming_query, dup_id=duplicate_id, orig_id=original_id)
                    
                    # Delete the duplicate node
                    delete_node_query = """
                    MATCH (duplicate)
                    WHERE id(duplicate) = $dup_id
                    DETACH DELETE duplicate
                    """
                    tx.run(delete_node_query, dup_id=duplicate_id)
                    deleted_count += 1
            
            processed += 1
            if processed % 50 == 0:
                print(f"  Processed {processed}/{len(duplicates_list)} UserIDs...")
        
        print(f"\nTotal duplicate Person nodes deleted: {deleted_count}")
        
        # Verify final count
        verify_query = """
        MATCH (n:Person)
        WHERE n.UserID IS NOT NULL
        WITH n.UserID AS user_id, count(*) AS count
        WHERE count > 1
        RETURN count(*) AS remaining_duplicates
        """
        result = session.run(verify_query)
        record = result.single()
        remaining = record["remaining_duplicates"] if record else 0
        print(f"Remaining duplicate Person nodes: {remaining}")
    
    driver.close()

if __name__ == "__main__":
    remove_duplicate_person_nodes()

