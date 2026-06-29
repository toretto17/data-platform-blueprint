#!/bin/bash
# ============================================================
# DynamoDB Config Loader
# ============================================================
# Uploads DDB JSON configs to S3 and triggers Lambda to load into DynamoDB.
# Renders ${environment} and ${account_id} placeholders before upload.
#
# Usage:
#   ENV=dev ./load_ddb_config.sh                    # Load all configs
#   ENV=prod ./load_ddb_config.sh config1.json      # Load specific file
#   ENV=dev ./load_ddb_config.sh --list             # List available configs
# ============================================================

set -e

# --- Configuration (override via env vars) ---
ENV="${ENV:-dev}"
ACCOUNT_ID="${ACCOUNT_ID:-}"
PROJECT="${PROJECT:-CHANGE_ME}"
FEATURE="${FEATURE:-CHANGE_ME}"
REGION="${REGION:-ap-southeast-1}"
S3_PREFIX="config/job"
LAMBDA_FUNCTION="lambda-${PROJECT}-fw-job-config-loader"

# --- Derive paths ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SOURCE_DIR="$REPO_ROOT/configs/$ENV/dynamodb"

# --- Derive ACCOUNT_ID from config if not set ---
if [[ -z "$ACCOUNT_ID" ]]; then
    ACCOUNT_ID=$(cd "$REPO_ROOT" && python3 -c "from src.common.constants.config import get_account_id; print(get_account_id('$ENV'))") || {
        echo "[ERROR] Could not derive ACCOUNT_ID for env '$ENV'" >&2; exit 1; }
fi

S3_BUCKET="s3-${PROJECT}-${FEATURE}-${ENV}-artifactory-${ACCOUNT_ID}"

echo "[INFO] Environment: $ENV | Account: $ACCOUNT_ID | Bucket: $S3_BUCKET"

# --- Functions ---
upload_file() {
    local file_path="$1"
    local file_name="$(basename "$file_path")"

    # Render placeholders
    local rendered=$(mktemp)
    python3 -c "
import sys
src, dst, env, acct, proj, feat = sys.argv[1:7]
s = open(src).read()
s = s.replace('\${environment}', env).replace('\${account_id}', acct)
s = s.replace('\${project}', proj).replace('\${feature}', feat)
open(dst, 'w').write(s)
" "$file_path" "$rendered" "$ENV" "$ACCOUNT_ID" "$PROJECT" "$FEATURE"

    echo "[INFO] Uploading: $file_name"
    aws s3 cp "$rendered" "s3://$S3_BUCKET/$S3_PREFIX/$file_name" --region "$REGION" --quiet
    rm -f "$rendered"
}

trigger_lambda() {
    echo "[INFO] Triggering Lambda: $LAMBDA_FUNCTION"
    aws lambda invoke \
        --function-name "$LAMBDA_FUNCTION" \
        --region "$REGION" \
        --payload '{}' \
        /tmp/lambda_response.json --quiet
    echo "[INFO] Lambda response: $(cat /tmp/lambda_response.json)"
}

# --- Main ---
case "${1:-}" in
    --list)
        echo "Available configs in $SOURCE_DIR:"
        find "$SOURCE_DIR" -name "*.json" -exec basename {} \; 2>/dev/null | sort
        ;;
    --help|-h)
        echo "Usage: ENV=dev $0 [file.json ...] [--list] [--s3-only]"
        ;;
    --s3-only)
        shift
        for f in "${@:-$(find "$SOURCE_DIR" -name "*.json")}"; do upload_file "$f"; done
        ;;
    *)
        # Upload and trigger
        if [[ $# -gt 0 ]]; then
            for f in "$@"; do upload_file "$SOURCE_DIR/$f"; done
        else
            for f in "$SOURCE_DIR"/*.json; do [[ -f "$f" ]] && upload_file "$f"; done
        fi
        trigger_lambda
        echo "[INFO] Done ✓"
        ;;
esac
