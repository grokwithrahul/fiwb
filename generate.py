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
CHAT_MODEL  = "mistral"

# ── Embedding + cosine ────────────────────────────────────────────────────────

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
        return pr_title

# ── State-Event Tracking ───────────────────────────────────────────────────────

def update_context_cube(conn, symbol_name, new_pr_data):
    """Checks if the PR changes the existing mapping (Event-State)."""
    # Ensure the row_factory is set for this local query
    conn.row_factory = sqlite3.Row
    current_state = conn.execute(
        "SELECT pr_number FROM context_cubes WHERE symbol = ?", 
        (symbol_name,)
    ).fetchone()

    if current_state and current_state['pr_number'] != new_pr_data['number']:
        event_type = "REDIRECTION_EVENT"
        print(f"Logic Shift Detected: {symbol_name} moved from PR {current_state['pr_number']} to {new_pr_data['number']}")
    else:
        event_type = "INITIAL_STATE"

    return {"event_state": event_type}

# ── Core: build function → intent mapping ─────────────────────────────────────

def build_context(conn: sqlite3.Connection, min_score: float = 0.55) -> dict:
    conn.row_factory = sqlite3.Row

    rows = conn.execute("SELECT source_type, source_id, vector FROM embeddings").fetchall()
    vectors = {(r["source_type"], r["source_id"]): json.loads(r["vector"]) for r in rows}

    functions = conn.execute("SELECT id, name, filename, start_line, body FROM functions").fetchall()

    context = defaultdict(list)
    seen_summaries = {}

    for fn in functions:
        fn_vec = vectors.get(("function", fn["id"]))
        if fn_vec is None: continue

        best_score, best_pr = 0.0, None
        for (stype, sid), vec in vectors.items():
            if stype != "pr": continue
            score = cosine(fn_vec, vec)
            if score > best_score:
                best_score, best_pr = score, sid

        if best_pr is None or best_score < min_score: continue

        pr = conn.execute(
            "SELECT number, title, body, merged_at, url FROM pull_requests WHERE number=?",
            (best_pr,)
        ).fetchone()
        if not pr: continue

        issues = conn.execute("""
            SELECT i.number, i.title FROM pr_issues pi
            JOIN issues i ON i.number = pi.issue_number
            WHERE pi.pr_number = ?
        """, (best_pr,)).fetchall()
        issue_titles = [i["title"] for i in issues]

        if best_pr not in seen_summaries:
            seen_summaries[best_pr] = summarise_intent(pr["title"], pr["body"], issue_titles)
        
        intent = seen_summaries[best_pr]

        # ── MAGIC: Event-State Detection ──
        state_metadata = update_context_cube(conn, fn["name"], pr)

        conn.execute("""
            INSERT OR REPLACE INTO context_cubes (symbol, pr_number, intent, event_state, last_updated)
            VALUES (?, ?, ?, ?, ?)
        """, (fn["name"], pr["number"], intent, state_metadata["event_state"], datetime.now().isoformat()))
        conn.commit()

        context[fn["filename"]].append({
            "function":    fn["name"],
            "line":        fn["start_line"],
            "intent":      intent,
            "event_state": state_metadata["event_state"],
            "pr_number":   pr["number"],
            "pr_title":    pr["title"],
            "pr_url":      pr["url"],
            "pr_date":     pr["merged_at"][:10] if pr["merged_at"] else "",
            "issues":      [{"number": i["number"], "title": i["title"]} for i in issues],
            "score":       round(best_score, 3),
        })

    return context

# ── Render .cursorrules ────────────────────────────────────────────────────────

def render_cursorrules(context: dict, repo: str) -> str:
    lines = [
        f"# Auto-generated context — {repo}",
        f"# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        f"# DO NOT EDIT MANUALLY",
        "",
        "## Codebase Intent Map",
        "",
    ]

    for filename in sorted(context.keys()):
        entries = sorted(context[filename], key=lambda x: -x["score"])
        lines.append(f"### {filename}")
        for e in entries:
            state_tag = " [LOGIC SHIFTED]" if e["event_state"] == "REDIRECTION_EVENT" else ""
            lines.append(f"- `{e['function']}()` (line {e['line']}){state_tag}")
            lines.append(f"  **Why it exists:** {e['intent']}")
            lines.append(f"  **Introduced by:** PR #{e['pr_number']} ({e['pr_date']})")
            lines.append("")

    return "\n".join(lines)

# ── Main ───────────────────────────────────────────────────────────────────────

def run(db_path: str, output: str, repo: str, min_score: float):
    conn = sqlite3.connect(db_path)
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
    
    print(f"Rendering {output}...")
    content = render_cursorrules(context, repo)

    with open(output, "w") as f:
        f.write(content)
    
    print(f"Done. Contextual Cubes active.")
    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",         default="context.db")
    parser.add_argument("--output",     default=".cursorrules")
    parser.add_argument("--repo",       default="dubinc/dub")
    parser.add_argument("--min-score", type=float, default=0.55)
    args = parser.parse_args()
    run(args.db, args.output, args.repo, args.min_score)
