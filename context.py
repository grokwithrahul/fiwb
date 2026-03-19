"""
context.py — Code-centric context generation.
Entry point is the file/function, not the PR.
All signals aggregated around the code entity.

Usage: python context.py --file apps/web/app/(ee)/api/messages/route.ts
       python context.py --function GET --file apps/web/app/(ee)/api/messages/route.ts
"""

import sqlite3
import argparse
import requests
from datetime import datetime


OLLAMA_CHAT = "http://localhost:11434/api/generate"
CHAT_MODEL  = "llama3.2"

SYNTHESIS_PROMPT = """Extract all useful information from the text below about why this code was changed.
Remove only markdown formatting, bot preambles, and irrelevant metadata.
Keep all substantive content. Do not summarize. Do not add anything.
[HUMAN] tagged lines are from engineers and are most valuable.
[BOT] tagged lines are from automated tools and are secondary.

{sources}

Extracted context:"""


def call_llm(prompt):
    try:
        r = requests.post(
            OLLAMA_CHAT,
            json={"model": CHAT_MODEL, "prompt": prompt, "stream": False, "temperature": 0},
            timeout=45,
        )
        r.raise_for_status()
        return r.json().get("response", "").strip()
    except Exception:
        return "Reason not documented."


def strip_boilerplate(text):
    if not text:
        return ""
    for marker in ["<!-- This is an auto-generated", "## Summary by CodeRabbit",
                   "<!-- end of auto-generated"]:
        if marker in text:
            text = text[:text.index(marker)]
    return text.strip()


def get_all_signals(conn, filename, function_name=None):
    """
    Aggregate all signals around a code entity.
    Entry point is file/function, not PR.
    """
    signals = []

    # ── PRs that touched this file ─────────────────────────────────────────────
    prs = conn.execute("""
        SELECT DISTINCT pr.number, pr.title, pr.body, pr.merged_at, pr.url
        FROM pr_files pf
        JOIN pull_requests pr ON pr.number = pf.pr_number
        WHERE pf.filename = ? AND pf.status != 'removed'
        ORDER BY pr.merged_at DESC
    """, (filename,)).fetchall()

    pr_numbers = [pr['number'] for pr in prs]

    if not pr_numbers:
        return [], []

    placeholders = ",".join("?" * len(pr_numbers))

    # ── PR bodies ──────────────────────────────────────────────────────────────
    for pr in prs:
        body = strip_boilerplate(pr['body'] or '')[:400]
        if body:
            signals.append(("pr_body", pr['number'], pr['merged_at'][:10], body))

    # ── Review comments on this specific file ──────────────────────────────────
    rc = conn.execute(f"""
        SELECT author, body, path, line, created_at, pr_number, source_confidence
        FROM pr_review_comments
        WHERE pr_number IN ({placeholders}) AND path = ?
        ORDER BY created_at ASC
    """, (*pr_numbers, filename)).fetchall()

    for c in rc:
        text = c['body'].strip()
        if len(text) > 15:
            stype = "review_comment_human" if c['source_confidence'] == 'human' else "review_comment_bot"
            signals.append((stype, c['pr_number'], c['created_at'][:10], text[:400]))

    # ── All review comments for these PRs (broader context) ───────────────────
    all_rc = conn.execute(f"""
        SELECT author, body, created_at, pr_number, source_confidence
        FROM pr_review_comments
        WHERE pr_number IN ({placeholders}) AND path != ?
        ORDER BY created_at ASC
        LIMIT 20
    """, (*pr_numbers, filename)).fetchall()

    for c in all_rc:
        text = c['body'].strip()
        if len(text) > 20:
            stype = "review_other_human" if c['source_confidence'] == 'human' else "review_other_bot"
            signals.append((stype, c['pr_number'], c['created_at'][:10], text[:300]))

    # ── Commit messages for these PRs ─────────────────────────────────────────
    commits = conn.execute(f"""
        SELECT sha, message, author, date, pr_number
        FROM commits
        WHERE pr_number IN ({placeholders})
        ORDER BY date ASC
    """, pr_numbers).fetchall()

    for c in commits:
        line = c['message'].split('\n')[0].strip()
        if len(line) > 10:
            signals.append(("commit", c['pr_number'], c['date'][:10], line[:200]))

    # ── Issues linked to these PRs ────────────────────────────────────────────
    issues = conn.execute(f"""
        SELECT DISTINCT i.number, i.title, i.body, pi.pr_number
        FROM pr_issues pi
        JOIN issues i ON i.number = pi.issue_number
        WHERE pi.pr_number IN ({placeholders})
    """, pr_numbers).fetchall()

    issue_numbers = [i['number'] for i in issues]

    for i in issues:
        body = strip_boilerplate(i['body'] or '')[:300]
        if i['title']:
            signals.append(("issue_title", i['pr_number'], "", i['title']))
        if body:
            signals.append(("issue_body", i['pr_number'], "", body))

    # ── Issue comments ────────────────────────────────────────────────────────
    if issue_numbers:
        issue_placeholders = ",".join("?" * len(issue_numbers))
        issue_comments = conn.execute(f"""
            SELECT ic.body, ic.author, ic.created_at, ic.issue_number
            FROM issue_comments ic
            WHERE ic.issue_number IN ({issue_placeholders})
            ORDER BY ic.created_at ASC
            LIMIT 20
        """, issue_numbers).fetchall()

        for c in issue_comments:
            text = c['body'].strip()
            if len(text) > 20:
                signals.append(("issue_comment", c['issue_number'], c['created_at'][:10], text[:300]))

    # ── PR-level reviews ──────────────────────────────────────────────────────
    reviews = conn.execute(f"""
        SELECT body, author, state, submitted_at, pr_number
        FROM pr_reviews
        WHERE pr_number IN ({placeholders})
        ORDER BY submitted_at ASC
    """, pr_numbers).fetchall()

    for r in reviews:
        text = (r['body'] or '').strip()
        if len(text) > 30:
            signals.append(("pr_review", r['pr_number'], r['submitted_at'][:10], text[:300]))

    # ── If function specified, filter to signals from PRs touching that function
    if function_name:
        fn_pr_numbers = set(row['pr_number'] for row in conn.execute(f"""
            SELECT DISTINCT pf.pr_number
            FROM pr_functions pf
            JOIN functions f ON f.id = pf.function_id
            WHERE f.name = ? AND f.filename = ?
        """, (function_name, filename)).fetchall())

        if fn_pr_numbers:
            signals = [s for s in signals if s[1] in fn_pr_numbers]

    return signals, prs


def format_signals(signals):
    if not signals:
        return "No signals found."

    parts = []
    for source_type, ref, date, text in signals:
        is_bot = "bot" in source_type
        label = "[BOT]" if is_bot else "[HUMAN]"
        date_str = " " + date if date else ""
        entry = label + date_str + "\n" + text.strip()
        parts.append(entry)

    return "\n\n".join(parts)


def get_entity_context(db_path, filename, function_name=None):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    signals, prs = get_all_signals(conn, filename, function_name)

    if not prs:
        conn.close()
        return f"## {filename}\n*No history found.*\n"

    entity = f"{filename}" + (f" → `{function_name}()`" if function_name else "")

    print(f"  Found {len(signals)} signals across {len(prs)} PRs", flush=True)
    print(f"  Synthesizing...", flush=True)

    why = format_signals(signals)

    # build output
    lines = [
        f"## {entity}",
        f"*{len(prs)} PRs, {len(signals)} signals — {datetime.now().strftime('%Y-%m-%d')}*",
        "",
        f"**Why this code exists:** {why}",
        "",
        "### PR History",
    ]

    for pr in prs:
        fns = conn.execute("""
            SELECT f.name, f.start_line
            FROM pr_functions pf
            JOIN functions f ON f.id = pf.function_id
            WHERE pf.pr_number = ? AND f.filename = ?
            ORDER BY f.start_line
        """, (pr['number'], filename)).fetchall()

        lines.append(f"- **PR #{pr['number']}** ({pr['merged_at'][:10]}): {pr['title']}")
        if fns:
            fn_str = ", ".join(f"`{f['name']}()`" for f in fns)
            lines.append(f"  Functions: {fn_str}")

    conn.close()
    return "\n".join(lines)


def get_repo_context(db_path, top_n_files=20):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    files = conn.execute("""
        SELECT filename, COUNT(DISTINCT pr_number) as pr_count
        FROM pr_files WHERE status != 'removed'
        GROUP BY filename ORDER BY pr_count DESC LIMIT ?
    """, (top_n_files,)).fetchall()

    conn.close()

    lines = [
        "# Codebase Context",
        f"*Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}*",
        "", "---", "",
    ]

    for f in files:
        lines.append(get_entity_context(db_path, f['filename']))
        lines.append("---")
        lines.append("")

    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",       default="context.db")
    parser.add_argument("--file",     help="file path")
    parser.add_argument("--function", help="specific function name")
    parser.add_argument("--top",      type=int, default=20)
    parser.add_argument("--output",   default=None)
    args = parser.parse_args()

    if args.file:
        content = get_entity_context(args.db, args.file, args.function)
    else:
        content = get_repo_context(args.db, args.top)

    if args.output:
        with open(args.output, "w") as f:
            f.write(content)
        print(f"Written to {args.output}")
    else:
        print(content)