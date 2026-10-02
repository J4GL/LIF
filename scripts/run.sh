#!/usr/bin/env bash
# Runs the DHT scraper temporarily, in this terminal.
# Everything stops and is removed when this script ends: Ctrl-C, closed terminal, kill, or the
# scraper stopping by itself. The SQLite database (Docker volume) is kept.
set -euo pipefail
# shellcheck source=scripts/common.sh
. "$(dirname "$0")/common.sh"

RUN_PROJECT="$DHT_PROJECT-run"
WATCHDOG=""
FOLLOWER=""
HUNG_UP=""

usage() {
    echo "usage: scripts/run.sh    (stop with Ctrl-C)"
}

# shellcheck disable=SC2329  # called by the EXIT trap
cleanup() {
    local status=$?
    set +e
    trap '' INT TERM HUP PIPE
    if [ -n "$HUNG_UP" ]; then
        mkdir -p "$DHT_LOG_DIR"
        exec >>"$DHT_LOG_DIR/run.log" 2>&1 </dev/null
    fi
    [ -n "$WATCHDOG" ] && kill "$WATCHDOG" 2>/dev/null
    [ -n "$FOLLOWER" ] && kill "$FOLLOWER" 2>/dev/null
    say "stopping the temporary run ($RUN_PROJECT)"
    compose "$RUN_PROJECT" kill -s SIGTERM >/dev/null 2>&1
    compose "$RUN_PROJECT" down --remove-orphans -t 30 >/dev/null 2>&1
    say "stopped; the database is kept in the volume $DHT_PROJECT-data"
    exit "$status"
}

# Removes the stack if this script dies without running its traps (kill -9).
start_watchdog() {
    local parent=$$
    (
        trap '' INT HUP
        while kill -0 "$parent" 2>/dev/null; do
            sleep 1
        done
        compose "$RUN_PROJECT" kill -s SIGTERM || true
        compose "$RUN_PROJECT" down --remove-orphans -t 30 || true
    ) >/dev/null 2>&1 &
    WATCHDOG=$!
}

main() {
    case "${1:-}" in
        "") ;;
        -h | --help)
            usage
            return 0
            ;;
        *)
            usage >&2
            return 2
            ;;
    esac
    require_docker
    ensure_env
    if [ -n "$(running_scraper "$DHT_PROJECT")" ]; then
        fail "the service $DHT_PROJECT is running; stop it first with: docker compose -p $DHT_PROJECT stop (or scripts/install-service.sh --uninstall)"
    fi
    if [ -n "$(running_scraper "$RUN_PROJECT")" ]; then
        fail "a temporary run ($RUN_PROJECT) is already running; stop it with Ctrl-C in its terminal or: docker compose -p $RUN_PROJECT down"
    fi
    ensure_volume
    DHT_RESTART=no
    # Its own image tag: a temporary build never replaces the service's image.
    DHT_IMAGE="${DHT_IMAGE:-dht-scraper:local}-run"
    DHT_REVISION=$(revision)
    export DHT_RESTART DHT_IMAGE DHT_REVISION
    compose "$RUN_PROJECT" down --remove-orphans >/dev/null 2>&1 || true
    compose "$RUN_PROJECT" build
    trap cleanup EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    trap 'HUNG_UP=1; exit 129' HUP
    start_watchdog
    compose "$RUN_PROJECT" up -d --wait --wait-timeout 300 --no-build --remove-orphans
    local scraper
    scraper=$(compose "$RUN_PROJECT" ps -q scraper)
    say "DHT scraper running at $(web_url "$RUN_PROJECT"); stop with Ctrl-C"
    docker logs -f "$scraper" &
    FOLLOWER=$!
    wait "$FOLLOWER" || true
    FOLLOWER=""
    return "$(docker inspect -f '{{.State.ExitCode}}' "$scraper" 2>/dev/null || echo 1)"
}

main "$@"; exit $?
