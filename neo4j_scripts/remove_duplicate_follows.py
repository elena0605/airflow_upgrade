#!/usr/bin/env python3
"""
Remove duplicate follows relationships.
Keeps one relationship per Person-Influencer-platform combination.
"""

from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")


def remove_duplicate_follows():
    """
    Remove duplicate :FOLLOWS relationships for the new schema.
    Dedupe key: (Person, Influencer, platform), where missing platform is treated separately.
    """
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    with driver.session() as session:
        check_query = """
        MATCH (p:Person)-[r:FOLLOWS]->(i:Influencer)
        WITH p, i, coalesce(r.platform, "__NO_PLATFORM__") AS platform_key, collect(r) AS relationships
    WHERE size(relationships) > 1
    WITH relationships[1..] AS duplicates
    UNWIND duplicates AS duplicate_rel
    RETURN count(duplicate_rel) AS will_be_deleted
    """
        result = session.run(check_query)
        record = result.single()
        will_be_deleted = record["will_be_deleted"] if record else 0

        print(f"Found {will_be_deleted} duplicate :FOLLOWS relationships to delete")

        if will_be_deleted == 0:
            print("No duplicate :FOLLOWS relationships found. Nothing to delete.")
        else:
            delete_query = """
            MATCH (p:Person)-[r:FOLLOWS]->(i:Influencer)
            WITH p, i, coalesce(r.platform, "__NO_PLATFORM__") AS platform_key, collect(r) AS relationships
            WHERE size(relationships) > 1
            UNWIND relationships[1..] AS duplicate_rel
            DELETE duplicate_rel
            RETURN count(duplicate_rel) AS deleted_count
            """

            with session.begin_transaction() as tx:
                result = tx.run(delete_query)
                record = result.single()
                deleted_count = record["deleted_count"] if record else 0
                print(f"Deleted {deleted_count} duplicate :FOLLOWS relationships")

        verify_total_query = "MATCH ()-[r:FOLLOWS]->() RETURN count(r) AS total"
        total = session.run(verify_total_query).single()["total"]
        print(f"Final total :FOLLOWS relationships: {total}")

        verify_by_platform_query = """
        MATCH ()-[r:FOLLOWS]->()
        RETURN coalesce(r.platform, "__NO_PLATFORM__") AS platform, count(r) AS total
        ORDER BY platform
        """
        rows = session.run(verify_by_platform_query)
        print("Breakdown by platform:")
        for row in rows:
            print(f"  {row['platform']}: {row['total']}")

    driver.close()

if __name__ == "__main__":
    remove_duplicate_follows()

