# shellcheck shell=bash
# Settings and helpers shared by run.sh, install-service.sh and update.sh. Sourced, never run.
# Works with /bin/bash 3.2 (macOS) and later.

DHT_REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DHT_PROJECT=${DHT_PROJECT:-dht-scraper}
DHT_ENV_FILE=${DHT_ENV_FILE:-$DHT_REPO/.env}
if [ -z "${DHT_LOG_DIR:-}" ]; then
    if [ "$(uname -s)" = Darwin ]; then
        DHT_LOG_DIR="$HOME/Library/Logs/dht-scraper"
    else
        DHT_LOG_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/dht-scraper"
    fi
fi
DOCKER_CLI_HINTS=false
export DHT_PROJECT DHT_ENV_FILE DHT_LOG_DIR DOCKER_CLI_HINTS
DHT_LOCK_DIR=""

say() {
    printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

fail() {
    printf '%s %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*" >&2
    exit 1
}

# compose PROJECT ARGS...: docker compose on this checkout, never with -f so compose.override.yaml loads.
compose() {
    local project=$1
    shift
    docker compose --project-directory "$DHT_REPO" --env-file "$DHT_ENV_FILE" -p "$project" "$@"
}

require_docker() {
    command -v docker >/dev/null 2>&1 || fail "docker is not installed"
    docker info >/dev/null 2>&1 || fail "Docker is not running: start Docker, then try again"
    docker compose version >/dev/null 2>&1 || fail "the docker compose plugin is missing"
}

# Creates the env file once (compose needs it); an existing file is never touched.
ensure_env() {
    [ -f "$DHT_ENV_FILE" ] && return 0
    (
        umask 077
        {
            printf '# Settings of the DHT scraper stack, created by scripts/common.sh.\n'
            printf '# DHT_WEB_BIND=0.0.0.0   (127.0.0.1 = this machine only)\n'
            printf '# DHT_WEB_PORT=8080\n'
        } >"$DHT_ENV_FILE"
    )
    chmod 600 "$DHT_ENV_FILE"
}

# The SQLite database lives in an external volume: `docker compose down -v` cannot delete it.
ensure_volume() {
    local volume="$DHT_PROJECT-data"
    docker volume inspect "$volume" >/dev/null 2>&1 || docker volume create "$volume" >/dev/null
}

# Prints the id of the running scraper container of a project, nothing when it does not run.
running_scraper() {
    compose "$1" ps -q --status running scraper 2>/dev/null || true
}

# The commit of the checkout, with -dirty when tracked files changed.
revision() {
    local commit
    commit=$(git -C "$DHT_REPO" rev-parse HEAD 2>/dev/null) || {
        echo unknown
        return 0
    }
    git -C "$DHT_REPO" diff --quiet HEAD -- 2>/dev/null || commit="$commit-dirty"
    echo "$commit"
}

# The web address of a project's scraper, from the port compose published.
web_url() {
    local address host port
    address=$(compose "$1" port scraper 8080 2>/dev/null | head -n 1)
    host=${address%:*}
    port=${address##*:}
    [ -n "$port" ] || port=${DHT_WEB_PORT:-8080}
    if [ "$host" = "0.0.0.0" ] || [ "$host" = "::" ]; then
        echo "http://localhost:$port/ (also from the local network)"
    else
        echo "http://localhost:$port/"
    fi
}

# One install or update at a time: a lock directory in .git holding the owner pid.
acquire_lock() {
    local git_dir owner
    git_dir=$(git -C "$DHT_REPO" rev-parse --absolute-git-dir 2>/dev/null) || git_dir="$DHT_REPO"
    DHT_LOCK_DIR="$git_dir/dht-scraper.lock"
    if mkdir "$DHT_LOCK_DIR" 2>/dev/null; then
        echo "$$" >"$DHT_LOCK_DIR/pid"
        return 0
    fi
    owner=$(cat "$DHT_LOCK_DIR/pid" 2>/dev/null || true)
    [ "$owner" = "$$" ] && return 0
    if [ -n "$owner" ] && kill -0 "$owner" 2>/dev/null; then
        DHT_LOCK_DIR=""
        fail "another dht-scraper script is running (pid $owner)"
    fi
    rm -rf "$DHT_LOCK_DIR"
    mkdir "$DHT_LOCK_DIR" 2>/dev/null || fail "cannot take the lock $DHT_LOCK_DIR"
    echo "$$" >"$DHT_LOCK_DIR/pid"
}

release_lock() {
    if [ -n "$DHT_LOCK_DIR" ] && [ "$(cat "$DHT_LOCK_DIR/pid" 2>/dev/null || true)" = "$$" ]; then
        rm -rf "$DHT_LOCK_DIR"
    fi
}
