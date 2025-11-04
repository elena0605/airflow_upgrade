import os
import sys
import pandas as pd
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Neo4j connection settings
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

# Paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_FILE_PATH = os.path.join(SCRIPT_DIR, "csv_files", "influencer_accounts.csv")


def validate_csv_data(df: pd.DataFrame) -> None:
    """Validate required columns and basic integrity."""
    required = ["influencer", "tiktok_account", "youtube_account"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    empty_names = df["influencer"].isna() | (df["influencer"].astype(str).str.strip() == "")
    if empty_names.any():
        raise ValueError(f"Empty influencer names at rows: {df[empty_names].index.tolist()}")


def test_neo4j_connection(driver) -> bool:
    try:
        with driver.session() as session:
            session.run("RETURN 1 AS ok").single()
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


def normalize_account(value: str) -> str | None:
    """Normalize account value - return None if empty or 'NA'."""
    if value is None:
        return None
    s = str(value).strip()
    if s == "" or s.upper() == "NA":
        return None
    return s


def main():
    try:
        if not os.path.exists(CSV_FILE_PATH):
            raise FileNotFoundError(f"CSV file not found: {CSV_FILE_PATH}")

        print(f"Reading CSV file: {CSV_FILE_PATH}")

        # The provided CSV uses ';' as delimiter
        df = pd.read_csv(CSV_FILE_PATH, sep=';', na_values=[], keep_default_na=False)

        validate_csv_data(df)

        print("Connecting to Neo4j...")
        driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        if not test_neo4j_connection(driver):
            sys.exit(1)

        # Cypher query to update influencer nodes with usernames
        cypher_update = """
        MATCH (i:Influencer {name: $name})
        SET i.tiktok_username = CASE WHEN $tiktok_username IS NOT NULL THEN $tiktok_username ELSE i.tiktok_username END,
            i.youtube_username = CASE WHEN $youtube_username IS NOT NULL THEN $youtube_username ELSE i.youtube_username END
        RETURN i.name AS name
        """

        updated_count = 0
        not_found_count = 0
        skipped_count = 0

        print("Updating Influencer nodes with TikTok and YouTube usernames...")
        with driver.session() as session:
            with session.begin_transaction() as tx:
                for idx, row in df.iterrows():
                    try:
                        name = str(row["influencer"]).strip()
                        tiktok_account = normalize_account(row.get("tiktok_account"))
                        youtube_account = normalize_account(row.get("youtube_account"))

                        # Skip if both accounts are empty/NA
                        if tiktok_account is None and youtube_account is None:
                            skipped_count += 1
                            continue

                        # Check if influencer node exists
                        check_query = "MATCH (i:Influencer {name: $name}) RETURN i.name AS name"
                        result = tx.run(check_query, name=name).single()

                        if result is None:
                            not_found_count += 1
                            print(f"Warning: Influencer '{name}' not found in Neo4j")
                            continue

                        # Update the node with usernames
                        tx.run(
                            cypher_update,
                            name=name,
                            tiktok_username=tiktok_account,
                            youtube_username=youtube_account
                        )
                        updated_count += 1

                        if (idx + 1) % 100 == 0:
                            print(f"Processed {idx + 1} rows...")

                    except Exception as e:
                        print(f"Error processing row {idx} (influencer: {row.get('influencer', 'Unknown')}): {e}")
                        continue

        print("Done.")
        print(f"Total rows processed: {len(df)}")
        print(f"Influencers updated: {updated_count}")
        print(f"Influencers not found: {not_found_count}")
        print(f"Rows skipped (both accounts NA/empty): {skipped_count}")

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
        try:
            driver.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()

