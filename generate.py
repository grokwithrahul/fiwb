"""
generate.py — Synthesize knowledge graph into .cursorrules context file.
Usage: python generate.py --db context.db --output .cursorrules

Generates a .cursorrules file that maps key functions to their intent,
the PRs that introduced them, and the issues that motivated those PRs.
Cursor reads this automatically on every prompt.
"""

import json
import sqlite3
import argparse
import requests
import math
from collections import defaultdict
from datetime import datetime


OLLAMA_URL  = "http://localhost:11434/api/embeddings"
OLLAMA_CHAT = "http://localhost:11434/api/generate"
EMBED_MODEL = "nomic-embed-text"
CHAT_MODEL  = "mistral"   # fallback: llama3, phi3 — whatever you have pulled


# ── Embedding + cosine (reused from query.py) ─────────────────────────────────

def embed(text):
    r = requests.post(OLLAMA_URL, json={"model": EMBED_MODEL, "prompt": text}, timeout=60)
    r.raise_for_status()
    return r.json()["embedding"]


def cosine(a, b):
    dot  = sum(x*y for x,y in zip(a,b))
    norm = math.sqrt(sum(x*x for x in a)) * math.sqrt(sum(x*x for x in b))
    return dot / norm if norm else 0.0


# ── Summarise a PR's intent via local LLM ─────────────────────────────────────

def summarise_intent(pr_title: str, pr_body: str, issue_titles: list[str]) -> str:
    """
    Ask local LLM to write a one-sentence intent summary for a PR.
    Falls back to pr_title if Ollama chat model isn't available.
    """
    issue_ctx = ""
    if issue_titles:
        issue_ctx = "Related issues: " + "; ".join(issue_titles)

    prompt = (
        f"In one sentence, explain the engineering intent behind this pull request.\n\n"
        f"Title: {pr_title}\n"
        f"{issue_ctx}\n"
        f"Body: {(pr_body or '')[:800]}\n\n"
        f"One sentence only, no preamble:"
    )
    try:
        r = requests.post(
            OLLAMA_CHAT,
            json={"model": CHAT_MODEL, "prompt": prompt, "stream": False},
            timeout=30,
        )
        r.raise_for_status()
        return r.json().get("response", pr_title).strip()
    except Exception:
        return pr_title   # graceful fallback


def create_context_cube(conn, fn_id, pr_number):
    """
    Synthesizes a Contextual Cube from the DB.
    """
    # 1. GATHER EVIDENCE (The 'Symptom')
    # Check how many times this specific function has been 'blamed' in bug-fix PRs
    friction_query = """
        SELECT COUNT(*) FROM pull_requests 
        WHERE number IN (SELECT pr_number FROM pr_files WHERE filename = (SELECT filename FROM functions WHERE id = ?))
        AND (title LIKE '%fix%' OR body LIKE '%bug%')
    """
    friction_count = conn.execute(friction_query, (fn_id,)).fetchone()[0]

    # 2. GATHER RATIONALE (The 'Written')
    pr_metadata = conn.execute("SELECT title, body FROM pull_requests WHERE number = ?", (pr_number,)).fetchone()

    # 3. CONSTRUCT THE CUBE
    return {
        "friction_level": "High" if friction_count > 3 else "Stable",
        "intent_source": pr_metadata['title'],
        "evidence_count": friction_count,
        "symbol_id": fn_id
    }

# ── Core: build function → intent mapping ─────────────────────────────────────

def build_context(conn: sqlite3.Connection, min_score: float = 0.55) -> dict:
    """
    For every function in the DB, find its most relevant PR and issue,
    then synthesise an intent summary.
    Returns: { filename: [ {function, intent, pr, issues, line} ] }
    """
    conn.row_factory = sqlite3.Row

    # load all embeddings once
    rows = conn.execute(
        "SELECT source_type, source_id, vector FROM embeddings"
    ).fetchall()
    vectors = {(r["source_type"], r["source_id"]): json.loads(r["vector"]) for r in rows}

    functions = conn.execute(
        "SELECT id, name, filename, start_line, body FROM functions"
    ).fetchall()

    context: dict = defaultdict(list)
    seen_summaries: dict[int, str] = {}   # pr_number → summary cache

    for fn in functions:
        fn_vec = vectors.get(("function", fn["id"]))
        if fn_vec is None:
            continue

        # find most relevant PR via embedding similarity
        best_score, best_pr = 0.0, None
        for (stype, sid), vec in vectors.items():
            if stype != "pr":
                continue
            score = cosine(fn_vec, vec)
            if score > best_score:
                best_score, best_pr = score, sid

        if best_pr is None or best_score < min_score:
            continue

        # get PR metadata
        pr = conn.execute(
            "SELECT number, title, body, merged_at, url FROM pull_requests WHERE number=?",
            (best_pr,)
        ).fetchone()
        if not pr:
            continue

        # get linked issues
        issues = conn.execute("""
            SELECT i.number, i.title FROM pr_issues pi
            JOIN issues i ON i.number = pi.issue_number
            WHERE pi.pr_number = ?
        """, (best_pr,)).fetchall()
        issue_titles = [i["title"] for i in issues]

        # synthesise intent (cached per PR)
        if best_pr not in seen_summaries:
            seen_summaries[best_pr] = summarise_intent(
                pr["title"], pr["body"], issue_titles
            )
        intent = seen_summaries[best_pr]

        context[fn["filename"]].append({
            "function":   fn["name"],
            "line":       fn["start_line"],
            "intent":     intent,
            "pr_number":  pr["number"],
            "pr_title":   pr["title"],
            "pr_url":     pr["url"],
            "pr_date":    pr["merged_at"][:10] if pr["merged_at"] else "",
            "issues":     [{"number": i["number"], "title": i["title"]} for i in issues],
            "score":      round(best_score, 3),
        })

    return context

def update_context_cube(conn, symbol_name, new_pr_data):
    """
    Checks if the 'Dummy' PR changes the existing mapping.
    Represented as an Event-State.
    """
    # 1. Fetch the CURRENT state of this Symbol
    current_state = conn.execute(
        "SELECT pr_number, intent FROM context_cubes WHERE symbol = ?", 
        (symbol_name,)
    ).fetchone()

    # 2. If a state exists and the new PR is different, we have a 'Transition'
    if current_state and current_state['pr_number'] != new_pr_data['number']:
        event_type = "REDIRECTION_EVENT"
        print(f"Logic Shift Detected: {symbol_name} moved from PR {current_state['pr_number']} to {new_pr_data['number']}")
    else:
        event_type = "INITIAL_STATE"

    # 3. Represent the Knowledge as a State-Change
    return {
        "symbol": symbol_name,
        "current_pr": new_pr_data['number'],
        "previous_pr": current_state['pr_number'] if current_state else None,
        "event_state": event_type,
        "timestamp": datetime.now().isoformat()
    }
# ── Render .cursorrules ────────────────────────────────────────────────────────

def render_cursorrules(context: dict, repo: str) -> str:
    lines = [
        f"# Auto-generated context — {repo}",
        f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"# DO NOT EDIT MANUALLY — regenerate with: python generate.py",
        "",
        "## Codebase Intent Map",
        "This file maps key functions to the engineering decisions that created them.",
        "Use this context when explaining, modifying, or debugging code.",
        "",
    ]

    for filename in sorted(context.keys()):
        entries = sorted(context[filename], key=lambda x: -x["score"])
        if not entries:
            continue

        lines.append(f"### {filename}")
        for e in entries:
            lines.append(f"- `{e['function']}()` (line {e['line']})")
            lines.append(f"  **Why it exists:** {e['intent']}")
            lines.append(f"  **Introduced by:** PR #{e['pr_number']} ({e['pr_date']}): {e['pr_title']}")
            if e["issues"]:
                issue_str = ", ".join(f"#{i['number']} {i['title']}" for i in e["issues"])
                lines.append(f"  **Motivated by:** {issue_str}")
            lines.append(f"  **Confidence:** {e['score']}")
            lines.append("")

    return "\n".join(lines)


# ── Main ───────────────────────────────────────────────────────────────────────

def run(db_path: str, output: str, repo: str, min_score: float):
    conn = sqlite3.connect(db_path)

    # NEW: Initialize the State-Event Table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS context_cubes (
            symbol TEXT PRIMARY KEY,
            pr_number INTEGER,
            intent TEXT,
            event_state TEXT,
            last_updated DATETIME
        )
    """)
    conn.commit()

    print("Building context map...")
    context = build_context(conn, min_score=min_score)

    total_fns = sum(len(v) for v in context.values())
    print(f"  Mapped {total_fns} functions across {len(context)} files")

    print(f"Rendering {output}...")
    content = render_cursorrules(context, repo)

    with open(output, "w") as f:
        f.write(content)

    size_kb = len(content.encode()) / 1024
    print(f"  Done. {output} written ({size_kb:.1f} KB)")
    print(f"\nDrop {output} in your repo root. Cursor picks it up automatically.")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",        default="context.db")
    parser.add_argument("--output",    default=".cursorrules")
    parser.add_argument("--repo",      default="dubinc/dub")
    parser.add_argument("--min-score", type=float, default=0.55)
    args = parser.parse_args()
    run(args.db, args.output, args.repo, args.min_score)
