import os
import atexit
from contextlib import contextmanager
from typing import Iterator, Optional

from neo4j import GraphDatabase, Driver, Session
from dotenv import load_dotenv


_driver: Optional[Driver] = None


def get_driver() -> Driver:
    """Return a singleton Neo4j Driver initialized from env/.env.

    Requires env variables:
      - NEO4J_URI (e.g., neo4j+s://<db-id>.databases.neo4j.io)
      - NEO4J_USER
      - NEO4J_PASSWORD
    """
    global _driver
    if _driver is None:
        load_dotenv()
        uri = os.environ.get("NEO4J_URI")
        user = os.environ.get("NEO4J_USER")
        password = os.environ.get("NEO4J_PASSWORD")
        if not uri or not user or not password:
            raise RuntimeError(
                "NEO4J_URI/NEO4J_USER/NEO4J_PASSWORD must be set in environment or .env"
            )
        _driver = GraphDatabase.driver(uri, auth=(user, password))
        # Fail fast if connectivity is not possible
        _driver.verify_connectivity()
        atexit.register(close_driver)
    return _driver


@contextmanager
def get_session(database: Optional[str] = None) -> Iterator[Session]:
    """Yield a Neo4j session bound to the optional database and close it on exit."""
    driver = get_driver()
    kwargs = {"database": database} if database else {}
    session = driver.session(**kwargs)
    try:
        yield session
    finally:
        try:
            session.close()
        except Exception:
            pass


def close_driver() -> None:
    """Close the global driver (registered with atexit)."""
    global _driver
    if _driver is not None:
        try:
            _driver.close()
        finally:
            _driver = None
