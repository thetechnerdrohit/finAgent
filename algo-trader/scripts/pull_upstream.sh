#!/usr/bin/env bash
# Pull helper — fetch UPSTREAM (Mayank's repo) and show what's new vs your branch.
# READ-ONLY: it only fetches + shows; YOU choose how to integrate. Pushing to
# upstream is impossible (push URL is disabled on the 'upstream' remote).
#
# Usage:  bash scripts/pull_upstream.sh [branch]      (default: algo-trader)
set -euo pipefail
BR="${1:-algo-trader}"

if ! git remote | grep -qx upstream; then
  echo "No 'upstream' remote. Add it (fetch-only) with:"
  echo "  git remote add upstream https://github.com/techfreakworm/finAgent.git"
  echo "  git remote set-url --push upstream DISABLED"
  exit 1
fi

git fetch upstream
cur=$(git rev-parse --abbrev-ref HEAD)
echo "=== new on upstream/$BR not in your branch ($cur) ==="
git log --oneline --no-merges "HEAD..upstream/$BR" || true
echo
echo "Integrate with ONE of (then: git push origin $cur):"
echo "  git rebase upstream/$BR     # replay your commits on top"
echo "  git merge  upstream/$BR     # create a merge commit"
echo "  git cherry-pick <sha>       # take specific commits only"
