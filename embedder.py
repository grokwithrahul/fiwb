"""
embedder.py — Embed PRs, issues, filenames, and function bodies.
Usage: python embedder.py --db context.db
"""

import json
import sqlite3
import argparse
import requests


OLLAMA_URL = "http://localhost:11434/api/embeddings"
MODEL      = "nomic-embed-text"


def init_embeddings_table(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS embeddings (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            source_type TEXT NOT NULL,
            source_id   INTEGER NOT NULL,
            text        TEXT NOT NULL,
            vector      TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_emb_unique
            ON embeddings(source_type, source_id);
    """)
    conn.commit()


def already_embedded(conn, source_type, source_id):
    return conn.execute(
        "SELECT 1 FROM embeddings WHERE source_type=? AND source_id=?",
        (source_type, source_id)
    ).fetchone() is not None


def embed(text):
    r = requests.post(OLLAMA_URL, json={"model": MODEL, "prompt": text}, timeout=60)
    r.raise_for_status()
    return r.json()["embedding"]


def save_embedding(conn, source_type, source_id, text, vector):
    conn.execute("""
        INSERT OR REPLACE INTO embeddings (source_type, source_id, text, vector)
        VALUES (?, ?, ?, ?)
    """, (source_type, source_id, text, json.dumps(vector)))


def run(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    init_embeddings_table(conn)

    # ── PRs ────────────────────────────────────────────────────────────────────
    prs = conn.execute("SELECT number, title, body FROM pull_requests").fetchall()
    print(f"Embedding {len(prs)} PRs...")
    for pr in prs:
        if already_embedded(conn, "pr", pr["number"]):
            continue
        text = f"PR: {pr['title']}\n\n{pr['body'] or ''}".strip()[:2000]
        save_embedding(conn, "pr", pr["number"], text, embed(text))
        conn.commit()
        print(f"  ✓ PR #{pr['number']}: {pr['title'][:50]}")

    # ── Issues ─────────────────────────────────────────────────────────────────
    issues = conn.execute("SELECT number, title, body FROM issues").fetchall()
    print(f"\nEmbedding {len(issues)} issues...")
    for issue in issues:
        if already_embedded(conn, "issue", issue["number"]):
            continue
        text = f"Issue: {issue['title']}\n\n{issue['body'] or ''}".strip()[:2000]
        save_embedding(conn, "issue", issue["number"], text, embed(text))
        conn.commit()
        print(f"  ✓ Issue #{issue['number']}: {issue['title'][:50]}")

    # ── Files ──────────────────────────────────────────────────────────────────
    files = conn.execute(
        "SELECT DISTINCT filename FROM pr_files WHERE status != 'removed'"
    ).fetchall()
    print(f"\nEmbedding {len(files)} filenames...")
    for i, f in enumerate(files):
        fname = f["filename"]
        fid   = i + 1
        if already_embedded(conn, "file", fid):
            continue
        save_embedding(conn, "file", fid, fname, embed(fname))
        if i % 10 == 0:
            conn.commit()
            print(f"  {i+1}/{len(files)} files...")
    conn.commit()

    # ── Functions (the important new layer) ────────────────────────────────────
    functions = conn.execute(
        "SELECT id, name, filename, body FROM functions"
    ).fetchall()
    print(f"\nEmbedding {len(functions)} functions...")
    for i, fn in enumerate(functions):
        if already_embedded(conn, "function", fn["id"]):
            continue
        # rich text: function name + file context + body
        text = f"function {fn['name']} in {fn['filename']}\n\n{fn['body'] or ''}".strip()[:2000]
        save_embedding(conn, "function", fn["id"], text, embed(text))
        if i % 20 == 0:
            conn.commit()
            print(f"  {i+1}/{len(functions)} functions...")
    conn.commit()

    total = conn.execute("SELECT COUNT(*) FROM embeddings").fetchone()[0]
    print(f"\nDone. {total} total embeddings in {db_path}")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="context.db")
    args = parser.parse_args()
    run(args.db)