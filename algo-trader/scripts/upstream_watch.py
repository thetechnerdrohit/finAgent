"""Watch the UPSTREAM repo (Mayank's) for new commits and alert via Telegram.

READ-ONLY: queries the public GitHub API anonymously. The upstream owner gets
NO notification (GitHub never notifies owners of reads/clones/API queries).

State is kept in data/upstream_watch.json (last-seen SHA per branch). On the
first run it records a baseline and sends a one-time "armed" message; after that
it pings the MAIN bot only when a watched branch advances.

Config (env / .env, with defaults):
  UPSTREAM_REPO      owner/repo            (default techfreakworm/finAgent)
  UPSTREAM_BRANCHES  comma-separated       (default main,algo-trader)
  TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID    (main alert bot)
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
BRANCHES = [b.strip() for b in os.getenv("UPSTREAM_BRANCHES", "main,algo-trader").split(",") if b.strip()]
STATE = ROOT / "data" / "upstream_watch.json"
TG_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TG_CHAT = os.getenv("TELEGRAM_CHAT_ID", "")


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


def main():
    state = {}
    if STATE.exists():
        try:
            state = json.loads(STATE.read_text())
        except Exception:
            state = {}
    STATE.parent.mkdir(parents=True, exist_ok=True)
    first_run = not state

    alerts = []
    for br in BRANCHES:
        try:
            commits = _gh(f"https://api.github.com/repos/{UPSTREAM}/commits?sha={urllib.parse.quote(br)}&per_page=10")
        except Exception as e:
            print(f"{br}: fetch failed ({e})")
            continue
        if not commits:
            continue
        latest = commits[0]["sha"]
        prev = state.get(br)
        state[br] = latest
        if prev and prev != latest:
            new = []
            for c in commits:
                if c["sha"] == prev:
                    break
                msg = c["commit"]["message"].splitlines()[0]
                who = c["commit"]["author"]["name"]
                new.append(f"- {msg} ({who})")
            cmp_url = f"https://github.com/{UPSTREAM}/compare/{prev[:7]}...{latest[:7]}"
            alerts.append(
                f"Upstream {UPSTREAM} [{br}]: {len(new)} new commit(s)\n"
                + "\n".join(new[:10]) + f"\n{cmp_url}")

    STATE.write_text(json.dumps(state, indent=2))

    if first_run:
        msg = (f"Upstream watch armed for {UPSTREAM} (branches: {', '.join(BRANCHES)}). "
               f"You'll be pinged here when Mayank pushes new commits.")
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
