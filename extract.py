"""
extract.py — Extract knowledge nodes from PR episodes using local LLM.
Usage: python extract.py --db context.db
Requires: ollama running with mistral or llama3 pulled.
"""

import json
import sqlite3
import argparse
import requests
import re
from datetime import datetime


OLLAMA_CHAT = "http://localhost:11434/api/generate"
CHAT_MODEL  = "mistral"  # fallback: llama3, phi3


EXTRACTION_PROMPT = """You are analyzing a pull request to extract organizational knowledge about a software codebase.

Pull Request:
{pr_content}

Linked Issues:
{issues_content}

Changed Functions:
{functions_content}

Extract all knowledge nodes from this PR. For each node output a JSON object with:
- "claim": one precise sentence stating what is known about this codebase
- "type": one of: Decision | Constraint | Requirement | AttemptedSolution | Fact
- "confidence": high | medium | low
- "reasoning": one sentence explaining why you believe this based on the evidence
- "function_names": list of function names this node explains (can be empty)

Rules:
- Decision: an architectural or product choice that was made
- Constraint: a technical limitation that forced a specific approach
- Requirement: a product requirement this PR was implementing
- AttemptedSolution: something that was tried or considered but not fully confirmed in the diff
- Fact: a general fact about how this part of the codebase works
- Mark confidence LOW when the PR title implies something the diff doesn't confirm
- Mark confidence HIGH only when the evidence directly states the claim
- Extract 2-5 nodes per PR, not more
- Each claim must be specific to this codebase, not generic

Respond with ONLY a JSON array of node objects. No preamble, no explanation, no markdown.
"""


def call_llm(prompt: str) -> str:
    r = requests.post(
        OLLAMA_CHAT,
        json={"model": CHAT_MODEL, "prompt": prompt, "stream": False},
        timeout=120,
    )
    r.raise_for_status()
    return r.json().get("response", "").strip()


def parse_nodes(response: str) -> list:
    """Extract JSON array from LLM response robustly."""
    # strip markdown code blocks if present
    response = re.sub(r"```(?:json)?", "", response).strip()
    # find first [ and last ]
    start = response.find("[")
    end   = response.rfind("]") + 1
    if start == -1 or end == 0:
        return []
    try:
        return json.loads(response[start:end])
    except json.JSONDecodeError:
        return []


def get_pr_context(conn, pr_episode: dict) -> tuple:
    """Get linked issues and changed functions for a PR episode."""
    meta     = json.loads(pr_episode["metadata"] or "{}")
    pr_num   = meta.get("number")

    # linked issues
    issues = conn.execute("""
        SELECT e.raw_content FROM pr_issues pi
        JOIN episodes e ON e.source_type = 'issue' AND e.source_id = CAST(pi.issue_number AS TEXT)
        WHERE pi.pr_number = ?
    """, (pr_num,)).fetchall()
    issues_content = "\n\n".join(i["raw_content"] for i in issues) or "None"

    # changed functions
    fns = conn.execute("""
        SELECT f.name, f.filename, f.start_line
        FROM pr_functions pf
        JOIN functions f ON f.id = pf.function_id
        WHERE pf.pr_number = ?
        LIMIT 10
    """, (pr_num,)).fetchall()
    functions_content = "\n".join(
        f"- {fn['name']}() in {fn['filename']}:{fn['start_line']}" for fn in fns
    ) or "None"

    return issues_content, functions_content


def already_extracted(conn, episode_id: int) -> bool:
    return conn.execute(
        "SELECT 1 FROM node_episodes WHERE episode_id = ?", (episode_id,)
    ).fetchone() is not None


def save_node(conn, node: dict, episode_id: int, pr_merged_at: str, function_ids: list):
    # insert node
    cur = conn.execute("""
        INSERT INTO nodes (type, claim, confidence, reasoning, valid_from)
        VALUES (?, ?, ?, ?, ?)
    """, (
        node.get("type", "Fact"),
        node.get("claim", ""),
        node.get("confidence", "medium"),
        node.get("reasoning", ""),
        pr_merged_at,
    ))
    node_id = cur.lastrowid

    # link to episode (provenance)
    conn.execute(
        "INSERT OR IGNORE INTO node_episodes (node_id, episode_id) VALUES (?, ?)",
        (node_id, episode_id)
    )

    # link to functions this node explains
    for fn_id in function_ids:
        conn.execute(
            "INSERT OR IGNORE INTO node_functions (node_id, function_id) VALUES (?, ?)",
            (node_id, fn_id)
        )

    return node_id


def run(db_path: str, limit: int):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # get all PR episodes not yet extracted
    pr_episodes = conn.execute("""
        SELECT e.* FROM episodes e
        WHERE e.source_type = 'pr'
        ORDER BY e.ingested_at DESC
        LIMIT ?
    """, (limit,)).fetchall()

    print(f"Extracting knowledge from {len(pr_episodes)} PR episodes...")
    total_nodes = 0

    for i, ep in enumerate(pr_episodes):
        if already_extracted(conn, ep["id"]):
            print(f"  [{i+1}/{len(pr_episodes)}] Already extracted, skipping")
            continue

        meta = json.loads(ep["metadata"] or "{}")
        pr_num = meta.get("number")
        print(f"  [{i+1}/{len(pr_episodes)}] PR #{pr_num}: {meta.get('title', '')[:50]}")

        issues_content, functions_content = get_pr_context(conn, dict(ep))

        prompt = EXTRACTION_PROMPT.format(
            pr_content=ep["raw_content"][:1500],
            issues_content=issues_content[:800],
            functions_content=functions_content,
        )

        response = call_llm(prompt)
        raw_nodes = parse_nodes(response)

        if not raw_nodes:
            print(f"    ⚠ No nodes extracted")
            # still mark as processed by inserting a placeholder
            conn.execute(
                "INSERT OR IGNORE INTO node_episodes (node_id, episode_id) SELECT -1, ?", (ep["id"],)
            )
            conn.commit()
            continue

        # get function IDs for linking
        fn_names = []
        for n in raw_nodes:
            fn_names.extend(n.get("function_names", []))

        fn_ids = []
        for name in set(fn_names):
            rows = conn.execute(
                "SELECT id FROM functions WHERE name = ?", (name,)
            ).fetchall()
            fn_ids.extend(r["id"] for r in rows)

        for node in raw_nodes:
            save_node(conn, node, ep["id"], meta.get("merged_at", ""), fn_ids)

        conn.commit()
        total_nodes += len(raw_nodes)
        print(f"    ✓ {len(raw_nodes)} nodes extracted")

    # ── Summary ────────────────────────────────────────────────────────────────
    counts = {
        "nodes":    conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0],
        "by_type":  conn.execute(
            "SELECT type, COUNT(*) as c FROM nodes GROUP BY type"
        ).fetchall(),
    }
    print(f"\nDone. {counts['nodes']} total nodes in knowledge graph:")
    for row in counts["by_type"]:
        print(f"  {row['type']}: {row['c']}")

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",    default="context.db")
    parser.add_argument("--limit", type=int, default=300)
    args = parser.parse_args()
    run(args.db, args.limit)