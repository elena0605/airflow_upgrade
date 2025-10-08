// === Nodes summary with counts ===
CALL db.labels() YIELD label
CALL {
    WITH label
    MATCH (n)
    WHERE label IN labels(n)
    RETURN count(n) AS node_count
}
RETURN 'Nodes' AS type, collect(label) AS items, sum(node_count) AS count;

// === Relationships summary with counts ===
CALL db.relationshipTypes() YIELD relationshipType
CALL {
    WITH relationshipType
    MATCH ()-[r]->()
    WHERE type(r) = relationshipType
    RETURN count(r) AS rel_count
}
RETURN 'Relationships' AS type, collect(relationshipType) AS items, sum(rel_count) AS count;

// === Property Keys summary ===
CALL db.propertyKeys() YIELD propertyKey
RETURN 'Property Keys' AS type, collect(propertyKey) AS items, count(*) AS count;










