"""
parser.py — Clone repo, AST-parse changed files per PR, store functions in SQLite.
Usage: python parser.py --db context.db --repo calcom/cal.com
Requires: pip install tree-sitter tree-sitter-python tree-sitter-typescript gitpython
"""

import os
import re
import sqlite3
import argparse
import subprocess
import json
from pathlib import Path


# ── DB ─────────────────────────────────────────────────────────────────────────

def init_functions_table(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS functions (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            filename    TEXT NOT NULL,
            name        TEXT NOT NULL,
            start_line  INTEGER,
            end_line    INTEGER,
            body        TEXT,
            language    TEXT
        );
        CREATE TABLE IF NOT EXISTS pr_functions (
            pr_number   INTEGER,
            function_id INTEGER,
            PRIMARY KEY (pr_number, function_id)
        );
        CREATE INDEX IF NOT EXISTS idx_fn_file ON functions(filename);
        CREATE INDEX IF NOT EXISTS idx_pr_fn   ON pr_functions(pr_number);
    """)
    conn.commit()


# ── Repo clone ─────────────────────────────────────────────────────────────────

def ensure_repo(repo: str, clone_dir: str = "./repo") -> str:
    if os.path.exists(clone_dir):
        print(f"  Repo already cloned at {clone_dir}, using existing.")
        return clone_dir
    print(f"  Cloning {repo} (shallow)...")
    subprocess.run(
        ["git", "clone", "--depth=1", f"https://github.com/{repo}.git", clone_dir],
        check=True
    )
    return clone_dir


# ── Language detection ─────────────────────────────────────────────────────────

def detect_language(filename: str):
    ext = Path(filename).suffix.lower()
    return {
        ".py":  "python",
        ".ts":  "typescript",
        ".tsx": "typescript",
        ".js":  "javascript",
        ".jsx": "javascript",
    }.get(ext)


# ── AST parsing ────────────────────────────────────────────────────────────────

def parse_python(source: str, filename: str) -> list[dict]:
    """Extract functions and methods using stdlib ast — no extra deps."""
    import ast
    functions = []
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return []

    lines = source.splitlines()

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            start = node.lineno
            end   = node.end_lineno or start
            body  = "\n".join(lines[start-1:end])[:1500]
            functions.append({
                "filename":   filename,
                "name":       node.name,
                "start_line": start,
                "end_line":   end,
                "body":       body,
                "language":   "python",
            })
    return functions


def parse_ts_js(source: str, filename: str, language: str) -> list[dict]:
    """
    Regex-based function extraction for TS/JS.
    Captures: function foo, const foo = (...) =>, async foo, export function foo
    Good enough for a prototype — handles 90% of real-world cases.
    """
    functions = []
    lines = source.splitlines()

    patterns = [
        # named function declarations
        r"^\s*(?:export\s+)?(?:async\s+)?function\s+(\w+)\s*[\(<]",
        # arrow functions assigned to const/let
        r"^\s*(?:export\s+)?(?:const|let)\s+(\w+)\s*=\s*(?:async\s*)?\(",
        # class methods
        r"^\s*(?:async\s+)?(\w+)\s*\([^)]*\)\s*(?::\s*\w+\s*)?\{",
    ]

    for i, line in enumerate(lines):
        for pattern in patterns:
            m = re.match(pattern, line)
            if m:
                name = m.group(1)
                if name in ("if", "for", "while", "switch", "catch"):
                    continue
                # grab up to 40 lines of body
                end = min(i + 40, len(lines))
                body = "\n".join(lines[i:end])[:1500]
                functions.append({
                    "filename":   filename,
                    "name":       name,
                    "start_line": i + 1,
                    "end_line":   end,
                    "body":       body,
                    "language":   language,
                })
                break  # one match per line

    return functions


def parse_file(filepath: str, filename: str) -> list[dict]:
    lang = detect_language(filename)
    if not lang:
        return []
    try:
        source = Path(filepath).read_text(errors="ignore")
    except (OSError, PermissionError):
        return []
    if lang == "python":
        return parse_python(source, filename)
    return parse_ts_js(source, filename, lang)


# ── Save ───────────────────────────────────────────────────────────────────────

def save_function(conn, fn: dict) -> int:
    # deduplicate by filename + name + start_line
    existing = conn.execute(
        "SELECT id FROM functions WHERE filename=? AND name=? AND start_line=?",
        (fn["filename"], fn["name"], fn["start_line"])
    ).fetchone()
    if existing:
        return existing[0]
    cur = conn.execute("""
        INSERT INTO functions (filename, name, start_line, end_line, body, language)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (fn["filename"], fn["name"], fn["start_line"],
          fn["end_line"], fn["body"], fn["language"]))
    return cur.lastrowid


# ── Main ───────────────────────────────────────────────────────────────────────

def run(db_path: str, repo: str, clone_dir: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    init_functions_table(conn)

    repo_path = ensure_repo(repo, clone_dir)

    # get all PR → file mappings from DB
    pr_files = conn.execute(
        "SELECT pr_number, filename FROM pr_files WHERE status != 'removed'"
    ).fetchall()

    # group by filename to avoid re-parsing
    file_to_prs: dict[str, list[int]] = {}
    for row in pr_files:
        file_to_prs.setdefault(row["filename"], []).append(row["pr_number"])

    print(f"Parsing {len(file_to_prs)} unique files...")
    total_fns = 0

    for filename, pr_numbers in file_to_prs.items():
        filepath = os.path.join(repo_path, filename)
        if not os.path.exists(filepath):
            continue

        functions = parse_file(filepath, filename)
        if not functions:
            continue

        for fn in functions:
            fn_id = save_function(conn, fn)
            for pr_num in pr_numbers:
                conn.execute(
                    "INSERT OR IGNORE INTO pr_functions VALUES (?, ?)",
                    (pr_num, fn_id)
                )

        total_fns += len(functions)

    conn.commit()

    stats = {
        "functions": conn.execute("SELECT COUNT(*) FROM functions").fetchone()[0],
        "pr_fn_links": conn.execute("SELECT COUNT(*) FROM pr_functions").fetchone()[0],
    }
    print(f"\nDone.")
    print(f"  {stats['functions']} functions parsed | {stats['pr_fn_links']} PR-function links")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db",    default="context.db")
    parser.add_argument("--repo",  default="calcom/cal.com")
    parser.add_argument("--clone", default="./repo")
    args = parser.parse_args()
    run(args.db, args.repo, args.clone)