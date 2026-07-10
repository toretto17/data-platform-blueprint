#!/usr/bin/env bash
# ============================================================
# Sync ML code to CodeCommit CI/CD Repositories
# ============================================================
#
# PURPOSE:
#   Syncs the MLOps source code from the main development repo into
#   separate CodeCommit BUILD and DEPLOY repos that CodePipeline watches.
#
# WHY SEPARATE REPOS?
#   CodePipeline triggers on push. We want:
#   - BUILD repo push → triggers pipeline upsert (training + inference)
#   - DEPLOY repo push → triggers model promotion + deploy
#   They have different triggers, different IAM, different lifecycles.
#
# WHAT GETS SYNCED:
#   BUILD repos:  common/ + model code + pipelines/ + config/ + utils/ + buildspec.yaml
#   DEPLOY repos: same + realtime/build.py + _monitoring_defaults.py
#
# WHAT GETS EXCLUDED:
#   - smoke.py (dev/QA tooling — not runtime code)
#   - tests/ directories (not needed in CI/CD repos)
#   - __pycache__/ (compiled Python)
#   - .venv/ (virtual environments)
#
# WHAT TO CHANGE:
#   1. MLOPS_DIR: Path to your mlops/ directory in the main repo
#   2. MAIN_REPO_DIR: Path to your main repo root (for config/, utils/)
#   3. CodeCommit URLs: Your actual repo URLs (get from AWS Console → CodeCommit)
#   4. BUILD/DEPLOY directory names: Match your cicd/ folder structure
#   5. DEV_ONLY_REMOVE: Add any other dev-only files to exclude
#
# PREREQUISITES:
#   - Git credentials for CodeCommit configured (HTTPS or SSH)
#   - For tooling account repos: export AWS creds for tooling account before running
#     export AWS_ACCESS_KEY_ID=...
#     export AWS_SECRET_ACCESS_KEY=...
#     export AWS_SESSION_TOKEN=...
#   - OR use a git credential helper configured for the tooling account
#
# USAGE:
#   ./sync_repos.sh
#
# ============================================================
set -euo pipefail

# ─── CHANGE THESE ───────────────────────────────────────────────────────────

# Path to your MLOps code in the main development repo
MLOPS_DIR="/path/to/your/repo/mlops"                    # CHANGE_ME

# Path to your main repo root (for shared modules like config/, utils/)
MAIN_REPO_DIR="/path/to/your/repo"                      # CHANGE_ME

# CodeCommit repository URLs (get from AWS Console → CodeCommit → Clone URL → HTTPS)
# Format: https://git-codecommit.{region}.amazonaws.com/v1/repos/{repo-name}
REGION="us-east-1"                                           # CHANGE_ME: your AWS region

# BUILD repos (one per model family)
declare -A BUILD_REPOS=(
    # ["cicd_subfolder"]="codecommit_repo_url"
    ["model_a_build"]="https://git-codecommit.${REGION}.amazonaws.com/v1/repos/CHANGE_ME-model-a-build"
    ["model_b_build"]="https://git-codecommit.${REGION}.amazonaws.com/v1/repos/CHANGE_ME-model-b-build"
)

# DEPLOY repos (one per model family)
declare -A DEPLOY_REPOS=(
    ["model_a_deploy"]="https://git-codecommit.${REGION}.amazonaws.com/v1/repos/CHANGE_ME-model-a-deploy"
    ["model_b_deploy"]="https://git-codecommit.${REGION}.amazonaws.com/v1/repos/CHANGE_ME-model-b-deploy"
)

# Files/patterns to REMOVE (dev-only, not runtime code)
DEV_ONLY_REMOVE='-name smoke.py -o -name __pycache__'

# Commit message prefix
COMMIT_MSG_PREFIX="sync"

# ────────────────────────────────────────────────────────────────────────────


# Helper function: clone repo, sync files, commit if changed, push
sync_repo() {
    local REPO_URL="$1"
    local CICD_SUBDIR="$2"     # e.g. "model_a_build" — matches cicd/ folder name
    local IS_DEPLOY="$3"       # "yes" or "no"

    echo ""
    echo "═══════════════════════════════════════════════════════════"
    echo "  Syncing: $CICD_SUBDIR"
    echo "═══════════════════════════════════════════════════════════"

    # Clone to temp directory
    cd /tmp && rm -rf repo
    git clone -q "$REPO_URL" repo
    cd repo

    # Remove all existing files (except .git)
    find . -maxdepth 1 ! -name .git ! -name . -exec rm -rf {} \; 2>/dev/null

    # ─── Copy common tree (shared across all repos) ─────────────────────────
    # CHANGE: Adjust these paths to match YOUR project structure
    cp -r "$MLOPS_DIR/common" .             # Shared utilities (aws/, pipeline_builder/, etc.)
    cp -r "$MLOPS_DIR/pipelines" .          # Pipeline definitions (train, inference)

    # Model-specific code — copy ALL model families (they reference each other's configs)
    # CHANGE: Replace with your actual model family directory names
    cp -r "$MLOPS_DIR/model_family_a" . 2>/dev/null || true   # CHANGE_ME
    cp -r "$MLOPS_DIR/model_family_b" . 2>/dev/null || true   # CHANGE_ME

    # Shared project modules (config, utils)
    cp -r "$MAIN_REPO_DIR/config" . 2>/dev/null || true
    cp -r "$MAIN_REPO_DIR/utils" . 2>/dev/null || true

    # ─── Remove dev-only files ──────────────────────────────────────────────
    rm -rf */tests utils/.venv 2>/dev/null || true
    find . \( $DEV_ONLY_REMOVE \) -exec rm -rf {} + 2>/dev/null || true

    # ─── Copy per-project files from cicd/ subfolder ────────────────────────
    cp "$MLOPS_DIR/cicd/$CICD_SUBDIR/buildspec.yaml" . 2>/dev/null || true
    cp "$MLOPS_DIR/cicd/$CICD_SUBDIR/cicd-requirements.txt" . 2>/dev/null || true

    # Deploy repos get additional files
    if [ "$IS_DEPLOY" = "yes" ]; then
        cp "$MLOPS_DIR/cicd/$CICD_SUBDIR/_monitoring_defaults.py" . 2>/dev/null || true
        mkdir -p realtime
        cp "$MLOPS_DIR/cicd/$CICD_SUBDIR/build.py" realtime/ 2>/dev/null || true
    fi

    # ─── Commit and push (only if changes exist) ────────────────────────────
    git add -A
    if git diff --cached --quiet; then
        echo "  (no changes — already in sync)"
    else
        git commit -q -m "${COMMIT_MSG_PREFIX} $(date -u +%Y%m%d_%H%M)"
        # Try common branch names
        git push -q origin HEAD:main 2>&1 || \
        git push -q origin HEAD:master 2>&1 || {
            # Fallback: detect default branch
            BRANCH=$(git symbolic-ref refs/remotes/origin/HEAD 2>/dev/null | sed 's|refs/remotes/origin/||')
            if [ -n "$BRANCH" ]; then
                git push -q origin "HEAD:$BRANCH"
            else
                echo "  ERROR: Could not determine default branch"
                return 1
            fi
        }
        echo "  ✓ Pushed changes"
    fi
}


# ─── Main execution ────────────────────────────────────────────────────────

echo "╔══════════════════════════════════════════════════════════╗"
echo "║  ML CI/CD Repository Sync                               ║"
echo "║  Source: $MLOPS_DIR"
echo "╚══════════════════════════════════════════════════════════╝"

# Sync BUILD repos
for SUBDIR in "${!BUILD_REPOS[@]}"; do
    sync_repo "${BUILD_REPOS[$SUBDIR]}" "$SUBDIR" "no"
done

# Sync DEPLOY repos
for SUBDIR in "${!DEPLOY_REPOS[@]}"; do
    sync_repo "${DEPLOY_REPOS[$SUBDIR]}" "$SUBDIR" "yes"
done

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║  ✅ Sync complete                                       ║"
echo "║  BUILD repos: ${#BUILD_REPOS[@]}                                          ║"
echo "║  DEPLOY repos: ${#DEPLOY_REPOS[@]}                                         ║"
echo "╚══════════════════════════════════════════════════════════╝"
