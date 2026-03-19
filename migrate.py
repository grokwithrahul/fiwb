"""
migrate.py — Migrate existing context.db to knowledge graph schema.
Run once: python migrate.py --db context.db
"""

import sqlite3
import argparse


SCHEMA = """
-- ── Episodes: raw source artifacts ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS episodes (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    source_type  TEXT NOT NULL,  -- pr | issue | function | file | prd | review
    source_id    TEXT NOT NULL,  -- original ID in source system
    raw_content  TEXT NOT NULL,  -- full raw text of the artifact
    metadata     TEXT,           -- JSON: url, author, date, filename, etc.
    ingested_at  TEXT DEFAULT (datetime('now')),
    UNIQUE(source_type, source_id)
);

-- ── Nodes: units of extracted knowledge ───────────────────────────────────────
CREATE TABLE IF NOT EXISTS nodes (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    type         TEXT NOT NULL,  -- Decision | Constraint | Requirement | AttemptedSolution | Fact
    claim        TEXT NOT NULL,  -- one sentence stating what is known
    confidence   TEXT NOT NULL,  -- high | medium | low
    reasoning    TEXT,           -- why the LLM believes this claim
    valid_from   TEXT,           -- when this became true
    valid_to     TEXT,           -- when this stopped being true (NULL = still valid)
    created_at   TEXT DEFAULT (datetime('now'))
);

-- ── Edges: typed relationships between nodes ──────────────────────────────────
CREATE TABLE IF NOT EXISTS edges (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    from_node_id INTEGER NOT NULL REFERENCES nodes(id),
    to_node_id   INTEGER NOT NULL REFERENCES nodes(id),
    type         TEXT NOT NULL,  -- IMPLEMENTS | SUPERSEDES | CONSTRAINS | ATTEMPTED | REJECTED | EXPLAINS
    confidence   TEXT NOT NULL,
    valid_from   TEXT,
    valid_to     TEXT,
    created_at   TEXT DEFAULT (datetime('now'))
);

-- ── Provenance: which episodes support which nodes ────────────────────────────
CREATE TABLE IF NOT EXISTS node_episodes (
    node_id      INTEGER NOT NULL REFERENCES nodes(id),
    episode_id   INTEGER NOT NULL REFERENCES episodes(id),
    PRIMARY KEY (node_id, episode_id)
);

-- ── Node-Function links: which functions does a node explain ──────────────────
CREATE TABLE IF NOT EXISTS node_functions (
    node_id      INTEGER NOT NULL REFERENCES nodes(id),
    function_id  INTEGER NOT NULL REFERENCES functions(id),
    PRIMARY KEY (node_id, function_id)
);

CREATE INDEX IF NOT EXISTS idx_nodes_type       ON nodes(type);
CREATE INDEX IF NOT EXISTS idx_edges_from       ON edges(from_node_id);
CREATE INDEX IF NOT EXISTS idx_edges_to         ON edges(to_node_id);
CREATE INDEX IF NOT EXISTS idx_episodes_source  ON episodes(source_type, source_id);
"""


def migrate(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    print("Creating knowledge graph schema...")
    conn.executescript(SCHEMA)
    conn.commit()

    # ── Migrate PRs to episodes ────────────────────────────────────────────────
    prs = conn.execute("SELECT * FROM pull_requests").fetchall()
    print(f"Migrating {len(prs)} PRs to episodes...")
    for pr in prs:
        import json
        raw = f"PR #{pr['number']}: {pr['title']}\n\n{pr['body'] or ''}"
        meta = json.dumps({
            "number": pr["number"],
            "title": pr["title"],
            "url": pr["url"],
            "merged_at": pr["merged_at"],
            "base_branch": pr["base_branch"],
            "author": pr["author"],
        })
        conn.execute("""
            INSERT OR IGNORE INTO episodes (source_type, source_id, raw_content, metadata, ingested_at)
            VALUES ('pr', ?, ?, ?, ?)
        """, (str(pr["number"]), raw, meta, pr["merged_at"]))

    # ── Migrate issues to episodes ─────────────────────────────────────────────
    issues = conn.execute("SELECT * FROM issues").fetchall()
    print(f"Migrating {len(issues)} issues to episodes...")
    for issue in issues:
        import json
        raw = f"Issue #{issue['number']}: {issue['title']}\n\n{issue['body'] or ''}"
        meta = json.dumps({
            "number": issue["number"],
            "title": issue["title"],
            "url": issue["url"],
            "state": issue["state"],
            "author": issue["author"],
        })
        conn.execute("""
            INSERT OR IGNORE INTO episodes (source_type, source_id, raw_content, metadata)
            VALUES ('issue', ?, ?, ?)
        """, (str(issue["number"]), raw, meta))

    # ── Migrate functions to episodes ──────────────────────────────────────────
    functions = conn.execute("SELECT * FROM functions").fetchall()
    print(f"Migrating {len(functions)} functions to episodes...")
    for fn in functions:
        import json
        raw = f"function {fn['name']} in {fn['filename']}:{fn['start_line']}\n\n{fn['body'] or ''}"
        meta = json.dumps({
            "function_id": fn["id"],
            "name": fn["name"],
            "filename": fn["filename"],
            "start_line": fn["start_line"],
            "language": fn["language"],
        })
        conn.execute("""
            INSERT OR IGNORE INTO episodes (source_type, source_id, raw_content, metadata)
            VALUES ('function', ?, ?, ?)
        """, (str(fn["id"]), raw, meta))

    conn.commit()

    # ── Summary ────────────────────────────────────────────────────────────────
    counts = {
        "episodes": conn.execute("SELECT COUNT(*) FROM episodes").fetchone()[0],
        "nodes":    conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0],
        "edges":    conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0],
    }
    print(f"\nMigration complete:")
    print(f"  {counts['episodes']} episodes | {counts['nodes']} nodes | {counts['edges']} edges")
    print(f"  Ready for extraction: python extract.py --db {db_path}")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", default="context.db")
    args = parser.parse_args()
    migrate(args.db)