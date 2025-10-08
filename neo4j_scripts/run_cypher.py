import sys
from neo4j import GraphDatabase

# Neo4j connection settings
NEO4J_URI = "bolt+ssc://rbl-neo4j.ecda.ai:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "neo4jneo4j"

def run_queries_from_file(file_path):
    # Read the Cypher file
    with open(file_path, "r") as f:
        content = f.read()

    # Split queries by semicolon and remove empty lines
    queries = [q.strip() for q in content.split(";") if q.strip()]

    # Connect to Neo4j
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    with driver.session() as session:
        for i, query in enumerate(queries, 1):
            # Extract explanation from comment lines
            explanation = ""
            for line in query.splitlines():
                line = line.strip()
                if line.startswith("//") or line.startswith("#"):
                    explanation = line
                    break

            if explanation:
                print(f"\n--- Query {i}: {explanation} ---\n")
            else:
                print(f"\n--- Running Query {i} ---\n")

            # Run the query
            result = session.run(query)

            # Print each record dynamically
            for record in result:
                record_dict = dict(record)
                output_parts = []
                for k, v in record_dict.items():
                    if isinstance(v, list):
                        v = ", ".join(str(x) for x in v)  # format lists nicely
                    output_parts.append(f"{k}: {v}")
                print(", ".join(output_parts))

    driver.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python run_cypher.py <cypher_file>")
        sys.exit(1)

    cypher_file = sys.argv[1]
    run_queries_from_file(cypher_file)






