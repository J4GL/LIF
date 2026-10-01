#!/usr/bin/env bash
# Installs the DHT scraper as a service: the compose stack restarts with Docker
# (restart: unless-stopped) and a daily cron job runs scripts/update.sh.
#   scripts/install-service.sh               install, or reinstall after a change
#   scripts/install-service.sh --uninstall   remove the container and the cron job, keep the database
set -euo pipefail
# shellcheck source=scripts/common.sh
. "$(dirname "$0")/common.sh"

CRON_MARKER="# dht-scraper-update:$DHT_REPO"

usage() {
    echo "usage: scripts/install-service.sh [--uninstall]"
}

# Single quotes for the shell that cron starts.
quoted() {
    printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\\\''/g")"
}

cron_line() {
    local update_log="$DHT_LOG_DIR/update.log"
    printf '17 4 * * * PATH=%s DHT_PROJECT=%s DHT_ENV_FILE=%s DHT_LOG_DIR=%s /bin/bash %s >> %s 2>&1 %s\n' \
        "$(quoted "$PATH")" "$(quoted "$DHT_PROJECT")" "$(quoted "$DHT_ENV_FILE")" "$(quoted "$DHT_LOG_DIR")" \
        "$(quoted "$DHT_REPO/scripts/update.sh")" "$(quoted "$update_log")" "$CRON_MARKER"
}

# The crontab without this checkout's line (crontab -l fails when the table is empty).
other_cron_lines() {
    { crontab -l 2>/dev/null || true; } | { grep -vF "$CRON_MARKER" || true; }
}

install_cron_line() {
    local line
    line=$(cron_line)
    case "$line" in
        *%*) fail "a path contains %, which cron cannot run: move the checkout or the logs" ;;
    esac
    { other_cron_lines; printf '%s\n' "$line"; } | crontab -
}

remove_cron_line() {
    local others
    others=$(other_cron_lines)
    if [ -n "$others" ]; then
        printf '%s\n' "$others" | crontab -
    else
        crontab -r 2>/dev/null || true
    fi
}

notes() {
    say "logs: docker compose -p $DHT_PROJECT logs -f scraper"
    say "daily update at 04:17, log in $DHT_LOG_DIR/update.log"
    if [ "$(uname -s)" = Darwin ]; then
        say "macOS: Docker Desktop must start when you sign in (Settings > General) for the service to come back after a restart"
        say "macOS: cron needs Full Disk Access (System Settings > Privacy & Security) when the checkout is on an external disk; a sleeping Mac skips the update"
    elif command -v systemctl >/dev/null 2>&1 && ! systemctl is-enabled --quiet docker 2>/dev/null; then
        say "Docker does not start at boot; enable it with: sudo systemctl enable docker"
    fi
}

install_service() {
    require_docker
    acquire_lock
    trap release_lock EXIT
    command -v crontab >/dev/null 2>&1 || fail "crontab is missing: install cron for the daily update"
    ensure_env
    if [ -n "$(running_scraper "$DHT_PROJECT-run")" ]; then
        fail "a temporary run ($DHT_PROJECT-run) is running: stop it (Ctrl-C in its terminal), then install"
    fi
    ensure_volume
    DHT_REVISION=$(revision)
    export DHT_REVISION
    say "installing the service $DHT_PROJECT at ${DHT_REVISION:0:12}"
    compose "$DHT_PROJECT" build
    compose "$DHT_PROJECT" up -d --wait --wait-timeout 300 --remove-orphans
    mkdir -p "$DHT_LOG_DIR"
    install_cron_line
    say "service running at $(web_url "$DHT_PROJECT")"
    notes
}

uninstall_service() {
    require_docker
    acquire_lock
    trap release_lock EXIT
    ensure_env
    compose "$DHT_PROJECT" down --remove-orphans
    if command -v crontab >/dev/null 2>&1; then
        remove_cron_line
    fi
    say "service removed; the database is kept in the volume $DHT_PROJECT-data"
    say "to delete the database too: docker volume rm $DHT_PROJECT-data"
}

main() {
    case "${1:-}" in
        "") install_service ;;
        --uninstall) uninstall_service ;;
        -h | --help) usage ;;
        *)
            usage >&2
            return 2
            ;;
    esac
}

main "$@"; exit $?
