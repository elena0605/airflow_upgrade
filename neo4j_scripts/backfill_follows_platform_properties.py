#!/usr/bin/env python3
"""
Backfill FOLLOWS relationship boolean platform flags from follows_with_flags.csv.

This script is intentionally separate from create_follows_dynamic.py.
It uses curated boolean columns only:
  - follows_tiktok
  - follows_youtube
"""

import os
import sys

import pandas as pd
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Load environment variables
load_dotenv()
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
FOLLOWS_FLAGS_CSV = os.path.join(SCRIPT_DIR, "follows_with_flags.csv")


def as_bool(value):
    """Parse flexible CSV booleans."""
    if pd.isna(value):
        return False
    normalized = str(value).strip().lower()
    return normalized in {"true", "1", "yes", "y"}


def backfill_platform_relationships():
    """Read follows_with_flags.csv and set platform booleans on :FOLLOWS."""
    if not NEO4J_PASSWORD:
        print("Error: NEO4J_PASSWORD environment variable not set")
        print("Create a .env file with:")
        print("  NEO4J_URI=bolt://localhost:7687")
        print("  NEO4J_USER=neo4j")
        print("  NEO4J_PASSWORD=your_password")
        sys.exit(1)

    if not os.path.exists(FOLLOWS_FLAGS_CSV):
        print(f"Error: CSV file not found: {FOLLOWS_FLAGS_CSV}")
        sys.exit(1)

    print(f"Reading {FOLLOWS_FLAGS_CSV}...")
    df = pd.read_csv(FOLLOWS_FLAGS_CSV, na_values=[], keep_default_na=False)

    required_columns = {"UserID", "Influencer", "follows_tiktok", "follows_youtube"}
    missing = required_columns.difference(df.columns)
    if missing:
        print(f"Error: Missing required columns: {sorted(missing)}")
        sys.exit(1)

    df = df[df["Influencer"].notna() & (df["Influencer"].str.strip() != "")]
    print(f"Found {len(df)} rows to process")

    print(f"Connecting to Neo4j at {NEO4J_URI}...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    update_query = """
    MATCH (p:Person {UserID: $user_id})
    MATCH (i:Influencer {name: $influencer})
    MERGE (p)-[r:FOLLOWS]->(i)
    SET
      r.follows_tiktok = coalesce(r.follows_tiktok, false) OR $follows_tiktok,
      r.follows_youtube = coalesce(r.follows_youtube, false) OR $follows_youtube
    """
    exists_query = """
    MATCH (p:Person {UserID: $user_id})
    MATCH (i:Influencer {name: $influencer})
    RETURN count(*) AS count
    """

    processed_count = 0
    updated_tiktok_true_count = 0
    updated_youtube_true_count = 0
    not_found_count = 0
    error_count = 0

    print("Backfilling FOLLOWS boolean platform flags...")
    with driver.session() as session:
        for idx, row in df.iterrows():
            try:
                user_id = int(row["UserID"])
                influencer = str(row["Influencer"]).strip()
                follows_tiktok = as_bool(row.get("follows_tiktok", False))
                follows_youtube = as_bool(row.get("follows_youtube", False))

                check_record = session.run(
                    exists_query, user_id=user_id, influencer=influencer
                ).single()
                if not check_record or check_record["count"] == 0:
                    not_found_count += 1
                    if not_found_count <= 10:
                        print(f"  Warning: Person {user_id} or Influencer '{influencer}' not found")
                    continue

                processed_count += 1
                session.run(
                    update_query,
                    user_id=user_id,
                    influencer=influencer,
                    follows_tiktok=follows_tiktok,
                    follows_youtube=follows_youtube,
                ).consume()
                if follows_tiktok:
                    updated_tiktok_true_count += 1
                if follows_youtube:
                    updated_youtube_true_count += 1

            except Exception as e:
                error_count += 1
                if error_count <= 10:
                    print(f"  Error processing row {idx}: {e}")
                continue

            if (idx + 1) % 1000 == 0:
                print(f"  Processed {idx + 1}/{len(df)} rows...")

    driver.close()

    print("\nVerifying relationships...")
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        total_follows = session.run("MATCH ()-[r:FOLLOWS]->() RETURN count(r) AS total").single()["total"]
        total_follows_tiktok = session.run(
            "MATCH ()-[r:FOLLOWS]->() WHERE coalesce(r.follows_tiktok, false) RETURN count(r) AS total"
        ).single()["total"]
        total_follows_youtube = session.run(
            "MATCH ()-[r:FOLLOWS]->() WHERE coalesce(r.follows_youtube, false) RETURN count(r) AS total"
        ).single()["total"]
    driver.close()

    print("\n" + "=" * 60)
    print("Summary:")
    print(f"  Person-Influencer pairs processed: {processed_count}")
    print(f"  Rows with follows_tiktok=True processed: {updated_tiktok_true_count}")
    print(f"  Rows with follows_youtube=True processed: {updated_youtube_true_count}")
    print(f"  Not found (Person/Influencer missing): {not_found_count}")
    print(f"  Errors: {error_count}")
    print(f"  Total FOLLOWS relationships in database: {total_follows}")
    print(f"  Total FOLLOWS with follows_tiktok=True: {total_follows_tiktok}")
    print(f"  Total FOLLOWS with follows_youtube=True: {total_follows_youtube}")
    print("=" * 60)


if __name__ == "__main__":
    try:
        backfill_platform_relationships()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nError: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
