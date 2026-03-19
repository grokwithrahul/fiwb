"""
query.py — Query the knowledge graph with plain English.
Usage: python query.py "how are links shortened"
"""

import json
import sqlite3
import argparse
import requests
import math


OLLAMA_URL = "http://localhost:11434/api/embeddings"
MODEL      = "nomic-embed-text"


def embed(text):
    r = requests.post(OLLAMA_URL, json={"model": MODEL, "prompt": text}, timeout=60)
    r.raise_for_status()
    return r.json()["embedding"]


def cosine(a, b):
    dot  = sum(x*y for x,y in zip(a,b))
    norm = math.sqrt(sum(x*x for x in a)) * math.sqrt(sum(x*x for x in b))
    return dot / norm if norm else 0.0


def query(db_path, question, top_k=5):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    q_vec = embed(question)

    rows = conn.execute(
        "SELECT source_type, source_id, text, vector FROM embeddings"
    ).fetchall()

    scored = []
    for row in rows:
        vec   = json.loads(row["vector"])
        score = cosine(q_vec, vec)
        scored.append((score, row["source_type"], row["source_id"], row["text"]))
    scored.sort(reverse=True)

    print(f"\n🔍 Query: {question}\n{'─'*60}")

    seen  = {"pr": set(), "issue": set(), "file": set(), "function": set()}
    shown = 0

    for score, stype, sid, text in scored:
        if shown >= top_k:
            break
        if sid in seen[stype]:
            continue
        seen[stype].add(sid)

        if stype == "function":
            fn = conn.execute(
                "SELECT id, name, filename, start_line FROM functions WHERE id=?", (sid,)
            ).fetchone()
            if not fn:
                continue
            print(f"[Function] score={score:.3f}")
            print(f"  {fn['name']}()  →  {fn['filename']}:{fn['start_line']}")
            # which PRs touched this function
            prs = conn.execute("""
                SELECT pr.number, pr.title, pr.merged_at
                FROM pr_functions pf
                JOIN pull_requests pr ON pr.number = pf.pr_number
                WHERE pf.function_id = ?
                LIMIT 3
            """, (sid,)).fetchall()
            if prs:
                print(f"  introduced/modified by:")
                for pr in prs:
                    print(f"    PR #{pr['number']} ({pr['merged_at'][:10]}): {pr['title'][:55]}")
            # linked issues via those PRs
            issues = conn.execute("""
                SELECT DISTINCT i.number, i.title
                FROM pr_functions pf
                JOIN pr_issues pi ON pi.pr_number = pf.pr_number
                JOIN issues i ON i.number = pi.issue_number
                WHERE pf.function_id = ?
                LIMIT 3
            """, (sid,)).fetchall()
            if issues:
                print(f"  linked issues:")
                for iss in issues:
                    print(f"    #{iss['number']}: {iss['title'][:55]}")

        elif stype == "pr":
            row = conn.execute(
                "SELECT number, title, url, merged_at FROM pull_requests WHERE number=?", (sid,)
            ).fetchone()
            if not row:
                continue
            print(f"[PR #{row['number']}] score={score:.3f}")
            print(f"  {row['title']}")
            print(f"  merged: {row['merged_at'][:10]}  →  {row['url']}")
            links = conn.execute(
                "SELECT issue_number FROM pr_issues WHERE pr_number=?", (sid,)
            ).fetchall()
            if links:
                print(f"  linked issues: {', '.join(f'#{l[0]}' for l in links)}")
            fns = conn.execute("""
                SELECT f.name, f.filename, f.start_line
                FROM pr_functions pf JOIN functions f ON f.id = pf.function_id
                WHERE pf.pr_number = ? LIMIT 5
            """, (sid,)).fetchall()
            if fns:
                print(f"  functions touched:")
                for fn in fns:
                    print(f"    • {fn['name']}()  {fn['filename']}:{fn['start_line']}")

        elif stype == "issue":
            row = conn.execute(
                "SELECT number, title, url, state FROM issues WHERE number=?", (sid,)
            ).fetchone()
            if not row:
                continue
            print(f"[Issue #{row['number']}] score={score:.3f}")
            print(f"  {row['title']}")
            print(f"  state: {row['state']}  →  {row['url']}")

        elif stype == "file":
            print(f"[File] score={score:.3f}")
            print(f"  {text}")
            fns = conn.execute(
                "SELECT name, start_line FROM functions WHERE filename=? LIMIT 4",
                (text,)
            ).fetchall()
            if fns:
                print(f"  functions: {', '.join(f'{f[0]}()' for f in fns)}")

        print()
        shown += 1

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",  default="context.db")
    parser.add_argument("--top", type=int, default=5)
    parser.add_argument("question", nargs="+")
    args = parser.parse_args()
    query(args.db, " ".join(args.question), top_k=args.top)