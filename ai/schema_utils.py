import neo4j
from neo4j.exceptions import ClientError
from typing import Any, Optional, Dict, List

# Optional project utility; ignore if unavailable
try:
    from utils import chat  # type: ignore
except Exception:
    chat = None  # not required for schema export


NODE_PROPERTIES_QUERY = """
CALL apoc.meta.data()
YIELD label, other, elementType, type, property
WHERE NOT type = "RELATIONSHIP" AND elementType = "node"
WITH label AS nodeLabels, collect({property:property, type:type}) AS properties
RETURN {labels: nodeLabels, properties: properties} AS output
"""

REL_PROPERTIES_QUERY = """
CALL apoc.meta.data()
YIELD label, other, elementType, type, property
WHERE NOT type = "RELATIONSHIP" AND elementType = "relationship"
WITH label AS relType, collect({property:property, type:type}) AS properties
RETURN {type: relType, properties: properties} AS output
"""

REL_QUERY = """
CALL apoc.meta.data()
YIELD label, other, elementType, type, property
WHERE type = "RELATIONSHIP" AND elementType = "node"
UNWIND other AS other_node
RETURN {start: label, type: property, end: toString(other_node)} AS output
"""


def query_database(
    driver: neo4j.Driver, query: str, params: dict[str, Any] = None
) -> list[dict[str, Any]]:
    if params is None:
        params = {}
    data = driver.execute_query(query, params)
    return [r.data() for r in data.records]


def _get_schema_via_apoc(driver: neo4j.Driver) -> dict[str, Any]:
    node_labels_response = driver.execute_query(NODE_PROPERTIES_QUERY)
    node_properties = [
        data["output"] for data in [r.data() for r in node_labels_response.records]
    ]

    rel_properties_query_response = driver.execute_query(REL_PROPERTIES_QUERY)
    rel_properties = [
        data["output"]
        for data in [r.data() for r in rel_properties_query_response.records]
    ]

    rel_query_response = driver.execute_query(REL_QUERY)
    relationships = [
        data["output"] for data in [r.data() for r in rel_query_response.records]
    ]

    return {
        "node_props": {el["labels"]: el["properties"] for el in node_properties},
        "rel_props": {el["type"]: el["properties"] for el in rel_properties},
        "relationships": relationships,
    }


def _get_schema_via_builtin(driver: neo4j.Driver) -> dict[str, Any]:
    """Fallback using db.schema.* procedures (no APOC required)."""
    # Node properties
    node_rows = driver.execute_query("CALL db.schema.nodeTypeProperties()")
    node_dict: Dict[str, List[Dict[str, Any]]] = {}
    for rec in node_rows.records:
        row = rec.data()
        label = row.get("nodeType") or "Unknown"
        prop_name = row.get("propertyName") or "property"
        prop_types = row.get("propertyTypes")
        prop_type_str = ",".join(prop_types) if isinstance(prop_types, list) else str(prop_types)
        node_dict.setdefault(label, []).append({"property": prop_name, "type": prop_type_str})

    # Relationship properties
    rel_rows = driver.execute_query("CALL db.schema.relTypeProperties()")
    rel_dict: Dict[str, List[Dict[str, Any]]] = {}
    for rec in rel_rows.records:
        row = rec.data()
        rtype = row.get("relType") or "RELATES_TO"
        prop_name = row.get("propertyName") or "property"
        prop_types = row.get("propertyTypes")
        prop_type_str = ",".join(prop_types) if isinstance(prop_types, list) else str(prop_types)
        rel_dict.setdefault(rtype, []).append({"property": prop_name, "type": prop_type_str})

    # Relationships (topology)
    rels: List[Dict[str, str]] = []
    # Use a data-driven scan to derive (start,type,end) triples with concrete labels
    relq = (
        "MATCH (a)-[r]->(b) "
        "RETURN DISTINCT head(labels(a)) AS start, type(r) AS type, head(labels(b)) AS end "
        "LIMIT 500"
    )
    rel_rows = driver.execute_query(relq)
    for rec in rel_rows.records:
        row = rec.data()
        start = row.get("start") or "Node"
        rtype = row.get("type") or "RELATES_TO"
        end = row.get("end") or "Node"
        rels.append({"start": str(start), "type": str(rtype), "end": str(end)})

    return {"node_props": node_dict, "rel_props": rel_dict, "relationships": rels}


def get_schema(
    driver: neo4j.Driver,
) -> str:
    structured_schema = get_structured_schema(driver)

    def _format_props(props: list[dict[str, Any]]) -> str:
        return ", ".join([f"{prop['property']}: {prop['type']}" for prop in props])

    formatted_node_props = [
        f"{label} {{{_format_props(props)}}}"
        for label, props in structured_schema["node_props"].items()
    ]

    formatted_rel_props = [
        f"{rel_type} {{{_format_props(props)}}}"
        for rel_type, props in structured_schema["rel_props"].items()
    ]

    formatted_rels = [
        f"(:{element['start']})-[:{element['type']}]->(:{element['end']})"
        for element in structured_schema["relationships"]
    ]

    return "\n".join(
        [
            "Node properties:",
            "\n".join(formatted_node_props),
            "Relationship properties:",
            "\n".join(formatted_rel_props),
            "The relationships:",
            "\n".join(formatted_rels),
        ]
    )


def get_structured_schema(driver: neo4j.Driver) -> dict[str, Any]:
    # Try APOC first; fallback to built-in procedures
    try:
        structured = _get_schema_via_apoc(driver)
    except ClientError as e:
        if "ProcedureNotFound" in str(e) or "There is no procedure" in str(e):
            structured = _get_schema_via_builtin(driver)
        else:
            raise

    # Also produce a human-readable multi-line string
    def _format_props(props: list[dict[str, Any]]) -> str:
        return ", ".join([f"{p.get('property')}: {p.get('type')}" for p in props])

    formatted_node_props = [
        f"{label} {{{_format_props(props)}}}"
        for label, props in structured.get("node_props", {}).items()
    ]

    formatted_rel_props = [
        f"{rel_type} {{{_format_props(props)}}}"
        for rel_type, props in structured.get("rel_props", {}).items()
    ]

    formatted_rels = [
        f"(:{element.get('start')})-[:{element.get('type')}]->(:{element.get('end')})"
        for element in structured.get("relationships", [])
    ]

    structured["formatted"] = "\n".join(
        [
            "Node properties:",
            "\n".join(formatted_node_props),
            "Relationship properties:",
            "\n".join(formatted_rel_props),
            "The relationships:",
            "\n".join(formatted_rels),
        ]
    )

    return structured