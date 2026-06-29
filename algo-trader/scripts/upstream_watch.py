"""Watch the UPSTREAM repo (Mayank's) for branch activity and alert via Telegram.

READ-ONLY: queries the public GitHub API anonymously. The upstream owner gets
NO notification (GitHub never notifies owners of reads/clones/API queries).

Watches ALL branches by default and pings on:
  * a NEW branch appearing,
  * any branch gaining new commits,
  * a branch being deleted.
Set UPSTREAM_BRANCHES to restrict to a specific comma-separated subset.

State: data/upstream_watch.json (last-seen SHA per branch). First run records a
baseline + sends a one-time "armed" message. Alerts go to the LOGGER bot.

Config (env / .env):
  UPSTREAM_REPO      owner/repo       (default techfreakworm/finAgent)
  UPSTREAM_BRANCHES  comma-separated  (default: empty = ALL branches)
  TELEGRAM_LOG_BOT_TOKEN / TELEGRAM_LOG_CHAT_ID  (LOGGER bot; falls back to main)
"""
import json
import os
import urllib.parse
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")

UPSTREAM = os.getenv("UPSTREAM_REPO", "techfreakworm/finAgent")
WATCH = [b.strip() for b in os.getenv("UPSTREAM_BRANCHES", "").split(",") if b.strip()]
STATE = ROOT / "data" / "upstream_watch.json"
TG_TOKEN = os.getenv("TELEGRAM_LOG_BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_LOG_CHAT_ID") or os.getenv("TELEGRAM_CHAT_ID", "")


def _gh(url):
    req = urllib.request.Request(url, headers={
        "Accept": "application/vnd.github+json", "User-Agent": "upstream-watch"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.load(r)


def _tg(text):
    if not TG_TOKEN or not TG_CHAT:
        print("telegram not configured — skipping")
        return
    data = urllib.parse.urlencode({
        "chat_id": TG_CHAT, "text": text, "disable_web_page_preview": "true"}).encode()
    try:
        urllib.request.urlopen(urllib.request.Request(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage", data=data), timeout=15)
    except Exception as e:
        print("telegram send failed:", e)


def _branches():
    """All branches on the upstream repo as {name: head_sha} (paginated)."""
    out, page = {}, 1
    while True:
        data = _gh(f"https://api.github.com/repos/{UPSTREAM}/branches?per_page=100&page={page}")
        if not data:
            break
        for b in data:
            out[b["name"]] = b["commit"]["sha"]
        if len(data) < 100:
            break
        page += 1
    return out


def _new_commits(branch, prev):
    """1-line messages for commits on `branch` newer than `prev` (up to 10)."""
    try:
        commits = _gh(f"https://api.github.com/repos/{UPSTREAM}/commits"
                      f"?sha={urllib.parse.quote(branch)}&per_page=10")
    except Exception:
        return []
    out = []
    for c in commits:
        if c["sha"] == prev:
            break
        out.append(f"- {c['commit']['message'].splitlines()[0]} ({c['commit']['author']['name']})")
    return out


def main():
    state = {}
    if STATE.exists():
        try:
            state = json.loads(STATE.read_text())
        except Exception:
            state = {}
    STATE.parent.mkdir(parents=True, exist_ok=True)
    first_run = not state

    try:
        current = _branches()
    except Exception as e:
        print(f"branch list failed: {e}")
        return
    if WATCH:
        current = {k: v for k, v in current.items() if k in WATCH}

    alerts = []
    if not first_run:
        for name, sha in current.items():
            prev = state.get(name)
            if prev is None:  # brand-new branch
                head = (_new_commits(name, None)[:1] or [f"@ {sha[:7]}"])[0]
                alerts.append(
                    f"NEW upstream branch [{name}]\n{head}\n"
                    f"https://github.com/{UPSTREAM}/tree/{urllib.parse.quote(name)}")
            elif prev != sha:  # branch advanced
                msgs = _new_commits(name, prev)
                alerts.append(
                    f"Upstream [{name}]: {len(msgs)} new commit(s)\n"
                    + "\n".join(msgs[:10])
                    + f"\nhttps://github.com/{UPSTREAM}/compare/{prev[:7]}...{sha[:7]}")
        for name in state:  # deleted branch
            if name not in current:
                alerts.append(f"Upstream branch DELETED [{name}]")

    STATE.write_text(json.dumps(current, indent=2))

    if first_run:
        scope = "ALL" if not WATCH else ",".join(WATCH)
        msg = (f"Upstream watch armed for {UPSTREAM} — watching {scope} branches "
               f"({len(current)} now). You'll be pinged on new / updated / deleted branches.")
        print(msg)
        _tg(msg)
    elif alerts:
        for a in alerts:
            print(a)
            _tg(a)
    else:
        print("no upstream changes")


if __name__ == "__main__":
    main()
