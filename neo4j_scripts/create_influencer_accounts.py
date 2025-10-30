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

        # Cypher snippets: use MERGE for idempotency; set platform on relationship
        # TikTok
        cypher_tiktok = (
            "MERGE (i:Influencer {name: $name}) "
            "WITH i "
            "OPTIONAL MATCH (t:TikTokUser {username: $username}) "
            "FOREACH (_ IN CASE WHEN t IS NULL THEN [] ELSE [1] END | "
            "  MERGE (i)-[r:HAS_ACCOUNT]->(t) "
            "  SET r.platform = 'TikTok' "
            ")"
        )

        # YouTube
        cypher_youtube = (
            "MERGE (i:Influencer {name: $name}) "
            "WITH i "
            "OPTIONAL MATCH (y:YouTubeChannel {username: $username}) "
            "FOREACH (_ IN CASE WHEN y IS NULL THEN [] ELSE [1] END | "
            "  MERGE (i)-[r:HAS_ACCOUNT]->(y) "
            "  SET r.platform = 'YouTube' "
            ")"
        )

        created = 0
        linked_tt = 0
        linked_yt = 0

        print("Creating Influencer nodes and HAS_ACCOUNT relationships...")
        with driver.session() as session:
            with session.begin_transaction() as tx:
                for idx, row in df.iterrows():
                    try:
                        name = str(row["influencer"]).strip()
                        tt = normalize_account(row.get("tiktok_account"))
                        yt = normalize_account(row.get("youtube_account"))

                        # Always MERGE influencer so it's present if any relationship exists
                        # We count new nodes approximately by attempting a MERGE and checking labels
                        tx.run("MERGE (:Influencer {name: $name})", name=name)
                        created += 1  # count rows processed for influencers

                        if tt is not None:
                            tx.run(cypher_tiktok, name=name, username=tt)
                            linked_tt += 1

                        if yt is not None:
                            tx.run(cypher_youtube, name=name, username=yt)
                            linked_yt += 1

                        if (idx + 1) % 100 == 0:
                            print(f"Processed {idx + 1} rows...")
                    except Exception as e:
                        print(f"Error processing row {idx} (influencer: {row.get('influencer', 'Unknown')}): {e}")
                        continue

        print("Done.")
        print(f"Influencers processed: {len(df)}")
        print(f"HAS_ACCOUNT created/merged: TikTok={linked_tt}, YouTube={linked_yt}")

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


