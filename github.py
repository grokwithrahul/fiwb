"""
github.py — Full GitHub ingestion: PRs, issues, review comments, commit messages.
Usage: python github.py --repo dubinc/dub --limit 50
Requires: GITHUB_TOKEN env var
"""

import os
import re
import sqlite3
import argparse
import requests


GITHUB_API = "https://api.github.com"


# ── DB ─────────────────────────────────────────────────────────────────────────

def init_db(path="context.db"):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS pull_requests (
            id          INTEGER PRIMARY KEY,
            number      INTEGER UNIQUE,
            title       TEXT,
            body        TEXT,
            author      TEXT,
            merged_at   TEXT,
            base_branch TEXT,
            url         TEXT
        );
        CREATE TABLE IF NOT EXISTS issues (
            id     INTEGER PRIMARY KEY,
            number INTEGER UNIQUE,
            title  TEXT,
            body   TEXT,
            author TEXT,
            state  TEXT,
            url    TEXT
        );
        CREATE TABLE IF NOT EXISTS issue_comments (
            id          INTEGER PRIMARY KEY,
            issue_number INTEGER,
            author      TEXT,
            body        TEXT,
            created_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS pr_issues (
            pr_number    INTEGER,
            issue_number INTEGER,
            PRIMARY KEY (pr_number, issue_number)
        );
        CREATE TABLE IF NOT EXISTS pr_files (
            id        INTEGER PRIMARY KEY AUTOINCREMENT,
            pr_number INTEGER,
            filename  TEXT,
            status    TEXT,
            additions INTEGER,
            deletions INTEGER,
            patch     TEXT
        );
        CREATE TABLE IF NOT EXISTS pr_review_comments (
            id          INTEGER PRIMARY KEY,
            pr_number   INTEGER,
            author      TEXT,
            body        TEXT,
            path        TEXT,
            line        INTEGER,
            created_at  TEXT
        );
        CREATE TABLE IF NOT EXISTS pr_reviews (
            id          INTEGER PRIMARY KEY,
            pr_number   INTEGER,
            author      TEXT,
            state       TEXT,
            body        TEXT,
            submitted_at TEXT
        );
        CREATE TABLE IF NOT EXISTS commits (
            sha         TEXT PRIMARY KEY,
            pr_number   INTEGER,
            message     TEXT,
            author      TEXT,
            date        TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_pr_files_pr      ON pr_files(pr_number);
        CREATE INDEX IF NOT EXISTS idx_pr_issues_pr     ON pr_issues(pr_number);
        CREATE INDEX IF NOT EXISTS idx_review_comments  ON pr_review_comments(pr_number);
        CREATE INDEX IF NOT EXISTS idx_commits_pr       ON commits(pr_number);
        CREATE INDEX IF NOT EXISTS idx_issue_comments   ON issue_comments(issue_number);
    """)
    conn.commit()
    return conn


# ── GitHub API ─────────────────────────────────────────────────────────────────

def gh(token, path, params=None):
    r = requests.get(
        f"{GITHUB_API}{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
        params=params or {},
        timeout=20,
    )
    r.raise_for_status()
    return r.json()


def extract_issue_numbers(text):
    if not text:
        return []
    return [int(n) for n in re.findall(r"#(\d+)", text)]


# ── Fetch ──────────────────────────────────────────────────────────────────────

def fetch_merged_prs(token, repo, limit):
    prs, page = [], 1
    while len(prs) < limit:
        batch = gh(token, f"/repos/{repo}/pulls", {
            "state": "closed", "sort": "updated",
            "direction": "desc",
            "per_page": min(100, limit - len(prs)),
            "page": page,
        })
        if not batch:
            break
        prs.extend([p for p in batch if p.get("merged_at")])
        page += 1
        if len(batch) < 100:
            break
    return prs[:limit]


def fetch_issue(token, repo, number):
    try:
        return gh(token, f"/repos/{repo}/issues/{number}")
    except requests.HTTPError:
        return None


def fetch_issue_comments(token, repo, number):
    try:
        return gh(token, f"/repos/{repo}/issues/{number}/comments", {"per_page": 100})
    except requests.HTTPError:
        return []


def fetch_pr_files(token, repo, number):
    try:
        return gh(token, f"/repos/{repo}/pulls/{number}/files", {"per_page": 100})
    except requests.HTTPError:
        return []


def fetch_pr_review_comments(token, repo, number):
    try:
        return gh(token, f"/repos/{repo}/pulls/{number}/comments", {"per_page": 100})
    except requests.HTTPError:
        return []


def fetch_pr_reviews(token, repo, number):
    try:
        return gh(token, f"/repos/{repo}/pulls/{number}/reviews", {"per_page": 100})
    except requests.HTTPError:
        return []


def fetch_pr_commits(token, repo, number):
    try:
        return gh(token, f"/repos/{repo}/pulls/{number}/commits", {"per_page": 100})
    except requests.HTTPError:
        return []


# ── Run ────────────────────────────────────────────────────────────────────────

def run(token, repo, limit, db_path):
    conn = init_db(db_path)
    print(f"Fetching up to {limit} merged PRs from {repo}...")
    prs = fetch_merged_prs(token, repo, limit)
    print(f"  Found {len(prs)} merged PRs")

    seen_issues = set()

    for i, pr in enumerate(prs, 1):
        num = pr["number"]
        print(f"  [{i}/{len(prs)}] PR #{num}: {pr['title'][:55]}")

        # ── PR ─────────────────────────────────────────────────────────────────
        conn.execute("""
            INSERT OR REPLACE INTO pull_requests
                (number, title, body, author, merged_at, base_branch, url)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (num, pr["title"], pr.get("body") or "",
              pr["user"]["login"], pr["merged_at"],
              pr["base"]["ref"], pr["html_url"]))

        # ── Linked issues ──────────────────────────────────────────────────────
        for iss_num in extract_issue_numbers(f"{pr['title']} {pr.get('body') or ''}"):
            conn.execute("INSERT OR IGNORE INTO pr_issues VALUES (?, ?)", (num, iss_num))
            if iss_num not in seen_issues:
                issue = fetch_issue(token, repo, iss_num)
                if issue and "pull_request" not in issue:
                    conn.execute("""
                        INSERT OR REPLACE INTO issues
                            (number, title, body, author, state, url)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (issue["number"], issue["title"],
                          issue.get("body") or "", issue["user"]["login"],
                          issue["state"], issue["html_url"]))

                    # issue comments — often contain the real why
                    comments = fetch_issue_comments(token, repo, iss_num)
                    for c in comments:
                        body = c.get("body", "").strip()
                        if not body or len(body) < 20:
                            continue
                        conn.execute("""
                            INSERT OR IGNORE INTO issue_comments
                                (id, issue_number, author, body, created_at)
                            VALUES (?, ?, ?, ?, ?)
                        """, (c["id"], iss_num, c["user"]["login"],
                              body, c["created_at"]))

                    seen_issues.add(iss_num)

        # ── Files ──────────────────────────────────────────────────────────────
        files = fetch_pr_files(token, repo, num)
        conn.executemany("""
            INSERT OR IGNORE INTO pr_files
                (pr_number, filename, status, additions, deletions, patch)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [(num, f["filename"], f["status"],
               f.get("additions", 0), f.get("deletions", 0),
               f.get("patch")) for f in files])

        # ── Review comments — highest signal for WHY ───────────────────────────
        BOT_AUTHORS = {"coderabbitai", "dependabot", "github-actions",
                       "renovate", "github-advanced-security"}

        review_comments = fetch_pr_review_comments(token, repo, num)
        for c in review_comments:
            body = c.get("body", "").strip()
            if not body or len(body) < 20:
                continue
            author = c["user"]["login"]
            confidence = "bot" if author.lower() in BOT_AUTHORS else "human"
            conn.execute("""
                INSERT OR IGNORE INTO pr_review_comments
                    (id, pr_number, author, body, path, line, created_at, source_confidence)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (c["id"], num, author, body,
                  c.get("path"), c.get("line"), c["created_at"], confidence))

        # ── PR-level reviews (body only, skip "LGTM" noise) ───────────────────
        reviews = fetch_pr_reviews(token, repo, num)
        for r in reviews:
            body = (r.get("body") or "").strip()
            if not body or len(body) < 30:
                continue
            author = r["user"]["login"]
            confidence = "bot" if author.lower() in BOT_AUTHORS else "human"
            conn.execute("""
                INSERT OR IGNORE INTO pr_reviews
                    (id, pr_number, author, state, body, submitted_at, source_confidence)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (r["id"], num, author,
                  r["state"], body, r["submitted_at"], confidence))

        # ── Commits — message often more descriptive than PR title ─────────────
        commits = fetch_pr_commits(token, repo, num)
        for c in commits:
            msg = c["commit"]["message"].strip()
            if not msg or len(msg) < 10:
                continue
            conn.execute("""
                INSERT OR IGNORE INTO commits
                    (sha, pr_number, message, author, date)
                VALUES (?, ?, ?, ?, ?)
            """, (c["sha"], num,
                  msg[:1000],
                  c["commit"]["author"]["name"],
                  c["commit"]["author"]["date"]))

        conn.commit()

    # ── Summary ────────────────────────────────────────────────────────────────
    s = lambda q: conn.execute(q).fetchone()[0]
    print(f"\nDone → {db_path}")
    print(f"  {s('SELECT COUNT(*) FROM pull_requests')} PRs")
    print(f"  {s('SELECT COUNT(*) FROM issues')} issues")
    print(f"  {s('SELECT COUNT(*) FROM issue_comments')} issue comments")
    print(f"  {s('SELECT COUNT(*) FROM pr_review_comments')} review comments")
    print(f"  {s('SELECT COUNT(*) FROM pr_reviews')} PR reviews")
    print(f"  {s('SELECT COUNT(*) FROM commits')} commits")
    print(f"  {s('SELECT COUNT(*) FROM pr_files')} file records")
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo",  default="dubinc/dub")
    parser.add_argument("--token", default=os.getenv("GITHUB_TOKEN"))
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--db",    default="context.db")
    args = parser.parse_args()
    if not args.token:
        raise SystemExit("Set GITHUB_TOKEN env var or pass --token")
    run(args.token, args.repo, args.limit, args.db)