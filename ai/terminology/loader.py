from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

try:
	import yaml  # type: ignore
except Exception:  # pragma: no cover
	yaml = None  # type: ignore


def load(version: str = "v1") -> Dict[str, Any]:
	"""Load terminology mapping dict from ai/terminology/<version>.yaml or .json.

	Falls back to JSON if YAML is unavailable.
	"""
	base_dir = Path(__file__).resolve().parent
	yaml_path = base_dir / f"{version}.yaml"
	json_path = base_dir / f"{version}.json"

	if yaml_path.exists() and yaml is not None:
		return yaml.safe_load(yaml_path.read_text())  # type: ignore
	if json_path.exists():
		return json.loads(json_path.read_text())
	raise FileNotFoundError(f"Terminology file not found for version '{version}' in {base_dir}")


def as_text(mapping: Dict[str, Any]) -> str:
	"""Render terminology mapping as multi-line YAML text (fallback to JSON)."""
	if yaml is not None:
		return yaml.dump(mapping, default_flow_style=False, sort_keys=False)  # type: ignore
	return json.dumps(mapping, indent=2, ensure_ascii=False)
