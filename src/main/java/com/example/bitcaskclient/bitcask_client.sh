set -euo pipefail

HOST="${BITCASK_HOST:-localhost}"
PORT="${BITCASK_PORT:-7070}"
BASE_URL="http://${HOST}:${PORT}"


usage() {
    echo "Usage:"
    echo "  $0 --view-all"
    echo "  $0 --view --key=<key>"
    echo "  $0 --perf --clients=<n>"
    exit 1
}

require_curl() {
    if ! command -v curl &>/dev/null; then
        echo "Error: curl is required but not installed." >&2
        exit 1
    fi
}

fetch_all_csv() {
    local outfile="$1"
    curl -sf "${BASE_URL}/all" > "${outfile}"
    echo "Written: ${outfile}"
}


MODE=""
KEY=""
CLIENTS=1

for arg in "$@"; do
    case "$arg" in
        --view-all)         MODE="view-all" ;;
        --view)             MODE="view" ;;
        --key=*)            KEY="${arg#--key=}" ;;
        --perf)             MODE="perf" ;;
        --clients=*)        CLIENTS="${arg#--clients=}" ;;
        -h|--help)          usage ;;
        *)                  echo "Unknown argument: $arg" >&2; usage ;;
    esac
done

require_curl


if ! curl -sf "${BASE_URL}/health" &>/dev/null; then
    echo "Error: BitCask server is not reachable at ${BASE_URL}" >&2
    exit 1
fi


case "$MODE" in

    view-all)
        TIMESTAMP=$(date +%s)
        OUTFILE="${TIMESTAMP}.csv"
        fetch_all_csv "$OUTFILE"
        ;;

    view)
        if [[ -z "$KEY" ]]; then
            echo "Error: --view requires --key=<key>" >&2
            usage
        fi
        ENCODED_KEY=$(python3 -c "import urllib.parse,sys; print(urllib.parse.quote(sys.argv[1]))" "$KEY" 2>/dev/null \
                      || echo "$KEY")
        RESULT=$(curl -sf "${BASE_URL}/get?key=${ENCODED_KEY}" || true)
        if [[ -z "$RESULT" ]]; then
            echo "Key not found: ${KEY}" >&2
            exit 1
        fi
        echo "$RESULT"
        ;;

    perf)
        if ! [[ "$CLIENTS" =~ ^[0-9]+$ ]] || [[ "$CLIENTS" -lt 1 ]]; then
            echo "Error: --clients must be a positive integer" >&2
            usage
        fi

        TIMESTAMP=$(date +%s)
        echo "Starting ${CLIENTS} concurrent client(s)..."

        worker() {
            local thread_num="$1"
            local outfile="${TIMESTAMP}_thread_${thread_num}.csv"
            curl -sf "${BASE_URL}/all" > "${outfile}"
            echo "[thread ${thread_num}] Written: ${outfile}"
        }
        export -f worker fetch_all_csv
        export BASE_URL TIMESTAMP

        PIDS=()
        for i in $(seq 1 "$CLIENTS"); do
            worker "$i" &
            PIDS+=($!)
        done

        FAILED=0
        for pid in "${PIDS[@]}"; do
            if ! wait "$pid"; then
                FAILED=$((FAILED + 1))
            fi
        done

        echo "All clients finished. Failed: ${FAILED}/${CLIENTS}"
        ;;

    *)
        echo "Error: no mode specified." >&2
        usage
        ;;
esac