#!/usr/bin/env bash
# Updates the DHT scraper from its git remote (GitHub). It checks the remote first and touches
# nothing when there is no new commit. With a new commit it fast-forwards the checkout, builds the
# new image while the service still runs, then restarts the service (only if it was running).
# A failed update puts the previous version back. Run by cron every day (install-service.sh).
#   scripts/update.sh
set -euo pipefail
# shellcheck source=scripts/common.sh
. "$(dirname "$0")/common.sh"

GIT_TERMINAL_PROMPT=0
GIT_HTTP_LOW_SPEED_LIMIT=1000
GIT_HTTP_LOW_SPEED_TIME=30
export GIT_TERMINAL_PROMPT GIT_HTTP_LOW_SPEED_LIMIT GIT_HTTP_LOW_SPEED_TIME

usage() {
    echo "usage: scripts/update.sh"
}

# The commit the service runs (its container's image), else the commit of the built image.
deployed_revision() {
    local container=$1 image=""
    if [ -n "$container" ]; then
        image=$(docker inspect -f '{{.Image}}' "$container" 2>/dev/null || true)
    fi
    [ -n "$image" ] || image=${DHT_IMAGE:-dht-scraper:local}
    docker image inspect -f '{{index .Config.Labels "org.opencontainers.image.revision"}}' "$image" 2>/dev/null || true
}

check() {
    say "checking for updates in $DHT_REPO"
    require_docker
    acquire_lock
    trap release_lock EXIT
    ensure_env
    git -C "$DHT_REPO" fetch --quiet || fail "git fetch failed: cannot reach the remote, nothing changed"
    local upstream head deployed
    upstream=$(git -C "$DHT_REPO" rev-parse '@{u}' 2>/dev/null) || fail "the branch has no upstream to update from"
    head=$(git -C "$DHT_REPO" rev-parse HEAD)
    deployed=$(deployed_revision "$(running_scraper "$DHT_PROJECT")")
    if [ "$head" = "$upstream" ] && [ "$deployed" = "$upstream" ]; then
        say "up to date at ${upstream:0:12}"
        return 0
    fi
    if [ "$head" != "$upstream" ]; then
        git -C "$DHT_REPO" diff --quiet HEAD -- || fail "local changes in tracked files: commit or stash them, then update again"
        git -C "$DHT_REPO" merge-base --is-ancestor HEAD "$upstream" || fail "the local branch has diverged from its upstream: merge it by hand"
        say "new version ${upstream:0:12} (running ${deployed:0:12}, checkout ${head:0:12})"
        git -C "$DHT_REPO" merge --ff-only --quiet "$upstream" || fail "the fast-forward failed, nothing changed"
        # The new version of this script builds and restarts: a changed procedure applies at once.
        exec /bin/bash "$DHT_REPO/scripts/update.sh" --apply "$head"
    fi
    say "finishing the update to ${upstream:0:12} (deployed ${deployed:0:12})"
    apply "$head"
}

apply() {
    local previous=$1 current running
    acquire_lock
    trap release_lock EXIT
    current=$(git -C "$DHT_REPO" rev-parse HEAD)
    running=$(running_scraper "$DHT_PROJECT")
    DHT_REVISION=$current
    export DHT_REVISION
    compose "$DHT_PROJECT" build --pull || rollback "$previous" "$running" "the new image did not build"
    if [ -n "$running" ]; then
        compose "$DHT_PROJECT" up -d --wait --wait-timeout 300 --remove-orphans || rollback "$previous" "$running" "the new version did not start"
        say "updated to ${current:0:12}, service restarted at $(web_url "$DHT_PROJECT")"
    else
        say "updated to ${current:0:12}; the service is not running, nothing started"
    fi
}

# Puts the previous commit and image back.
rollback() {
    local previous=$1 running=$2 reason=$3
    printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "update failed: $reason; going back to ${previous:0:12}" >&2
    if [ "$(git -C "$DHT_REPO" rev-parse HEAD)" != "$previous" ]; then
        git -C "$DHT_REPO" reset --hard --quiet "$previous"
    fi
    DHT_REVISION=$(revision)
    export DHT_REVISION
    compose "$DHT_PROJECT" build || say "rebuilding the previous version failed too" >&2
    if [ -n "$running" ]; then
        compose "$DHT_PROJECT" up -d --wait --wait-timeout 300 || say "restarting the previous version failed too" >&2
    fi
    exit 1
}

main() {
    case "${1:-}" in
        "") check ;;
        --apply)
            [ -n "${2:-}" ] || {
                usage >&2
                return 2
            }
            apply "$2"
            ;;
        -h | --help) usage ;;
        *)
            usage >&2
            return 2
            ;;
    esac
}

main "$@"; exit $?
