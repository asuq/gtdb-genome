#!/usr/bin/env bash

# Run the prepared remote validation suite through simple on-server presets.

set -u
set -o pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
REMOTE_RUNNER="${REAL_DATA_SERVER_REMOTE_RUNNER:-${SCRIPT_DIR}/run-real-data-tests-remote.sh}"


usage() {
    cat <<'EOF'
Usage:
  bin/run-real-data-tests-server.sh
  bin/run-real-data-tests-server.sh smoke
  bin/run-real-data-tests-server.sh full
  bin/run-real-data-tests-server.sh full-large
  bin/run-real-data-tests-server.sh C1 C5 C6

Presets:
  smoke       Run C1 C4 C6
  full        Run the default remote suite
  full-large  Run the default remote suite with RUN_OPTIONAL_LARGE=1

Any explicit case IDs are passed through to run-real-data-tests-remote.sh.
EOF
}


die() {
    printf 'ERROR: %s\n' "$*" >&2
    exit 2
}


main() {
    local mode="${1:-smoke}"

    case "${mode}" in
        -h | --help)
            usage
            return 0
            ;;
        smoke)
            if [ "$#" -gt 1 ]; then
                die "Preset 'smoke' does not accept extra arguments"
            fi
            exec bash "${REMOTE_RUNNER}" C1 C4 C6
            ;;
        full)
            if [ "$#" -gt 1 ]; then
                die "Preset 'full' does not accept extra arguments"
            fi
            exec bash "${REMOTE_RUNNER}"
            ;;
        full-large)
            if [ "$#" -gt 1 ]; then
                die "Preset 'full-large' does not accept extra arguments"
            fi
            export RUN_OPTIONAL_LARGE=1
            exec bash "${REMOTE_RUNNER}"
            ;;
        C*)
            exec bash "${REMOTE_RUNNER}" "$@"
            ;;
        *)
            usage >&2
            die "Unknown preset or case list: ${mode}"
            ;;
    esac
}


main "$@"
