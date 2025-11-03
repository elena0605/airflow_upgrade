from pathlib import Path
import os
import sys

# Ensure project root is on sys.path for absolute imports
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils_neo4j import get_driver, close_driver, get_session
from schema_utils import get_schema, get_structured_schema
from ai.terminology.loader import load as load_terminology, as_text as terminology_as_text
from ai.examples.loader import load_text as load_examples_text

try:
    import yaml  # type: ignore
except Exception:
    yaml = None  # type: ignore

try:
    from openai import OpenAI  # type: ignore
except Exception:
    OpenAI = None  # type: ignore

try:
    from dotenv import load_dotenv  # type: ignore
except Exception:
    load_dotenv = None  # type: ignore


def run_cypher(query: str):
    # Open a new session and execute the query, return list of dict rows
    with get_session() as session:  # type: ignore
        result = session.run(query)
        return [record.data() for record in result]


def summarize_results(question: str, cypher: str, rows) -> str:
    # Use OpenAI if available, otherwise naive text summary
    api_key = os.environ.get("OPENAI_API_KEY")
    model = (
        os.environ.get("OPEN_AI_MODEL")
        or os.environ.get("OPENAI_MODEL")
        or "gpt-4o"
    )
    if OpenAI is not None and api_key:
        client = OpenAI(api_key=api_key)
        preview = rows[:10] if isinstance(rows, list) else rows
        import json as _json
        prompt = (
            "You are a helpful data analyst. Answer the user's question using the query "
            "results below. Be concise, conversational, and avoid code. If numeric, format "
            "with commas. If tabular, show a compact markdown table (max 10 rows).\n\n"
            f"Question: {question}\n\nCypher Used:\n{cypher}\n\n"
            f"Results (first 10 rows):\n{_json.dumps(preview, ensure_ascii=False)}\n\n"
            "Provide the answer now."
        )
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=600,
        )
        return (resp.choices[0].message.content or "").strip()

    # Fallback naive summary
    if not rows:
        return "No results found."
    if isinstance(rows, list) and len(rows) == 1 and isinstance(rows[0], dict) and len(rows[0]) == 1:
        key, value = next(iter(rows[0].items()))
        return f"{key}: {value}"
    count = len(rows) if isinstance(rows, list) else 0
    return f"Returned {count} rows. Showing first: {rows[:1]}"


def render_template(template_text: str, variables: dict[str, str]) -> str:
    rendered = template_text
    for key, value in variables.items():
        rendered = rendered.replace(f"{{{{ {key} }}}}", value)
    return rendered


def main() -> None:
    # 1) Get schema (formatted string)
    # Load .env from project root early so OPEN_AI_KEY/OPEN_AI_MODEL are available
    if load_dotenv is not None:
        try:
            load_dotenv(dotenv_path=str(ROOT / ".env"))
        except Exception:
            pass

    driver = get_driver()
    try:
        structured = get_structured_schema(driver)
        schema_string = structured.get("formatted") or get_schema(driver)
    finally:
        try:
            close_driver()
        except Exception:
            pass

    # Schema-only mode (CLI flag or env var)
    if any(arg in {"--schema", "-s"} for arg in sys.argv[1:]) or \
       str(os.environ.get("SCHEMA_ONLY", "")).lower() in {"1", "true", "yes"}:
        print(schema_string)
        return

    # 2) Load terminology and examples as text blocks
    terminology_dict = load_terminology("v1")
    terminology_str = terminology_as_text(terminology_dict)

    examples_str = load_examples_text(
        "v1", prompt_id="graph.text_to_cypher", include_tags=None, limit=None
    )

    # 3) Load prompt template YAML and render
    prompt_path = ROOT / "ai" / "prompts" / "text_to_cypher_v1.yaml"
    if yaml is None:
        raise RuntimeError("PyYAML is required to load the prompt YAML. Please install 'pyyaml'.")
    prompt_cfg = yaml.safe_load(prompt_path.read_text())  # type: ignore
    template_text = prompt_cfg.get("template", "")
    params = prompt_cfg.get("params", {}) or {}

    # Prefer CLI positional argument: python3 text_to_cypher.py "Your question here"
    if len(sys.argv) > 1:
        question = " ".join(sys.argv[1:])
    else:
        question = os.environ.get("QUESTION", "Return 10 A to B relationships")

    rendered = render_template(
        template_text,
        {
            "schema": schema_string,
            "terminology": terminology_str,
            "examples": examples_str,
            "question": question,
        },
    )

    # Optionally debug-print the rendered prompt
    if os.environ.get("DEBUG_PROMPT", "").lower() in {"1", "true", "yes"}:
        print(rendered)

    # 4) If OpenAI is available and API key is set, call the model
    # Prefer OPEN_AI_* (project naming), fallback to OPENAI_* (SDK naming)
    api_key = os.environ.get("OPENAI_API_KEY")
    if OpenAI is not None and api_key:
        client = OpenAI(api_key=api_key)
        model = (
            os.environ.get("OPEN_AI_MODEL")
            or os.environ.get("OPENAI_MODEL")
            or "gpt-4o"
        )
        temperature = float(params.get("temperature", 0.0))
        max_tokens = int(params.get("max_tokens", 1200))

        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": rendered}],
            temperature=temperature,
            max_tokens=max_tokens,
        )
        output = (response.choices[0].message.content or "").strip()
        print(output)
        # Execute returned Cypher by default; allow opt-out via env flag
        flag = os.environ.get("EXECUTE_CYPHER") or os.environ.get("RUN_CYPHER")
        execute = True if flag is None else str(flag).lower() not in {"0", "false", "no"}
        if execute and output:
            try:
                rows = run_cypher(output)
                mode = (os.environ.get("OUTPUT_MODE") or "json").lower()
                import json as _json
                if mode in {"json", "both"}:
                    print(_json.dumps(rows, indent=2, ensure_ascii=False))
                if mode in {"chat", "both"}:
                    summary = summarize_results(question, output, rows)
                    print(summary)
            except Exception as e:
                print(f"Execution error: {e}", file=sys.stderr)
        return

    # Fallback: if no OpenAI, print the rendered prompt (for inspection/copy-paste)
    print(rendered)


if __name__ == "__main__":
    main()