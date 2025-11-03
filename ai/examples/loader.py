from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

try:
    import yaml  # type: ignore
except Exception:  # pragma: no cover
    yaml = None  # type: ignore


def load(version: str = "v1") -> Dict[str, Any]:
    """Load examples mapping from ai/examples/<version>.yaml.

    Returns a dict with keys: "prompt_id" and "examples": List[Dict[id, tags, snippet]]
    """
    base_dir = Path(__file__).resolve().parent
    yaml_path = base_dir / f"{version}.yaml"
    if not yaml_path.exists():
        raise FileNotFoundError(f"Examples file not found for version '{version}' in {base_dir}")
    if yaml is None:
        raise RuntimeError("PyYAML is required to load examples YAML. Please install 'pyyaml'.")
    return yaml.safe_load(yaml_path.read_text())  # type: ignore


def _matches_tags(item_tags: Iterable[str], include_tags: Optional[Iterable[str]]) -> bool:
    if not include_tags:
        return True
    tags_set = set(t.lower() for t in item_tags)
    include_set = set(t.lower() for t in include_tags)
    return bool(tags_set & include_set)


def select_examples(
    mapping: Dict[str, Any],
    *,
    prompt_id: Optional[str] = None,
    ids: Optional[Iterable[str]] = None,
    include_tags: Optional[Iterable[str]] = None,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    examples: List[Dict[str, Any]] = mapping.get("examples", [])
    mapping_prompt_id: Optional[str] = mapping.get("prompt_id")

    if prompt_id:
        def allowed(e: Dict[str, Any]) -> bool:
            e_pids = e.get("prompt_ids")
            if e_pids:
                return prompt_id in e_pids
            if mapping_prompt_id:
                return prompt_id == mapping_prompt_id
            return True
        examples = [e for e in examples if allowed(e)]

    if ids:
        idset = set(ids)
        examples = [e for e in examples if e.get("id") in idset]

    if include_tags:
        examples = [e for e in examples if _matches_tags(e.get("tags", []), include_tags)]

    if limit is not None:
        examples = examples[:limit]

    return examples


def as_text(examples: List[Dict[str, Any]]) -> str:
    """Render examples for {{ examples }}.

    Preferred format (Question/Cypher pairs):
      Question: <question>\nCypher: <cypher>
    Joined with a single newline between pairs to match the requested format.

    Fallback: join 'snippet' fields with two newlines if question/cypher not present.
    """
    pairs: List[str] = []
    for e in examples:
        q = str(e.get("question", "")).strip()
        c = str(e.get("cypher", "")).strip()
        if q and c:
            pairs.append(f"Question: {q}\nCypher: {c}")

    if pairs:
        # Match user's example: "\n".join([...f"Question...\nCypher..."...])
        return "\n".join(pairs)

    # Fallback to snippet style
    snippets = [e.get("snippet", "").strip() for e in examples if e.get("snippet")]
    return "\n\n".join(snippets)


def load_text(
    version: str = "v1",
    *,
    prompt_id: Optional[str] = None,
    ids: Optional[Iterable[str]] = None,
    include_tags: Optional[Iterable[str]] = None,
    limit: Optional[int] = None,
) -> str:
    mapping = load(version)
    selected = select_examples(
        mapping,
        prompt_id=prompt_id,
        ids=ids,
        include_tags=include_tags,
        limit=limit,
    )
    return as_text(selected)
