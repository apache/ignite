#!/bin/bash

# Get the absolute path to the project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../../../" && pwd)"

# Change to the project root directory
cd "$PROJECT_ROOT" || exit 1

# Save current git state (branch name and commit hash)
ORIGINAL_BRANCH=$(git symbolic-ref --short HEAD 2>/dev/null || echo "")
ORIGINAL_COMMIT=$(git rev-parse HEAD)

# Track whether Dockerfiles were patched (for cleanup on exit)
DOCKERFILES_PATCHED=0
PATCHED_DOCKERFILES=""

# Function to restore original Dockerfiles
restore_dockerfiles() {
    if [ "$DOCKERFILES_PATCHED" -eq 1 ] && [ -n "$PATCHED_DOCKERFILES" ]; then
        echo -e "\nRestoring patched Dockerfiles"
        git checkout "$ORIGINAL_COMMIT" -- $PATCHED_DOCKERFILES
    fi
}

# Function to restore original git state
restore_git_state() {
    if [ -n "$ORIGINAL_BRANCH" ]; then
        echo -e "\nRestoring git state to branch: $ORIGINAL_BRANCH"
        git checkout "$ORIGINAL_BRANCH" 2>/dev/null
    else
        echo -e "\nRestoring git state to detached commit: $ORIGINAL_COMMIT"
        git checkout "$ORIGINAL_COMMIT" 2>/dev/null
    fi
    restore_dockerfiles
}

# Set trap to restore git state on exit (success or failure)
trap restore_git_state EXIT

# Check that commit hash is provided, or use the latest commit in current branch
if [ $# -eq 1 ]; then
    COMMIT_HASH=$1
elif [ $# -eq 0 ]; then
    COMMIT_HASH=$(git rev-parse HEAD)
else
    echo "Usage: $0 [commit_hash]"
    exit 1
fi

# Perform git checkout to the specified commit
echo -e "\nPerforming git checkout to commit: $COMMIT_HASH"
git checkout "$COMMIT_HASH"

# Build the project (skip if distribution archive already exists)
SOURCE_ARCHIVE="$PROJECT_ROOT/target/bin/apache-ignite-*.zip"

if ls $SOURCE_ARCHIVE 1> /dev/null 2>&1; then
    echo -e "\nDistribution archive already found, skipping build steps"
else
    echo -e "\nBuilding the project: ./mvnw clean install -T1C -Pall-java,licenses -DskipTests"
    ./mvnw clean install -T1C -Pall-java,licenses -DskipTests

    # Check success of previous command
    if [ $? -ne 0 ]; then
        echo -e "\nError during project build"
        exit 1
    fi

    # Initialize the release
    echo -e "\nInitializing release: ./mvnw initialize -Prelease"
    ./mvnw initialize -Prelease

    # Check success of previous command
    if [ $? -ne 0 ]; then
        echo -e "\nError during release initialization"
        exit 1
    fi
fi

# Copy and unpack the release archive
echo -e "\nCopying and unpacking the release archive"
# Detect CPU architecture
ARCH=$(uname -m)
case "$ARCH" in
    arm64|aarch64)
        TARGET_DIR="$PROJECT_ROOT/deliveries/docker/apache-ignite/arm64"
        ;;
    x86_64)
        TARGET_DIR="$PROJECT_ROOT/deliveries/docker/apache-ignite/x86_64"
        ;;
    *)
        echo "Unsupported architecture: $ARCH"
        exit 1
        ;;
esac

# Check if archive exists
if [ ! -f $(echo $SOURCE_ARCHIVE) ]; then
    echo -e "\nArchive not found: $SOURCE_ARCHIVE"
    exit 1
fi

# Copy archive to target directory
cp $(echo $SOURCE_ARCHIVE) "$TARGET_DIR/"

# Unpack the archive
cd "$TARGET_DIR"
unzip -o "$(basename $(echo $SOURCE_ARCHIVE))"

# Return to project root directory
cd "$PROJECT_ROOT"

# Copy the startup script
echo -e "\nCopying startup script"
cp "$PROJECT_ROOT/deliveries/docker/apache-ignite/run.sh" "$TARGET_DIR/"

# Patch Dockerfiles: switch apk repos to HTTP (bypasses TLS cert issues in corporate networks)
PATCHED_DOCKERFILES=$(find "$PROJECT_ROOT/deliveries/docker/apache-ignite" -name Dockerfile)
for DF in $PATCHED_DOCKERFILES; do
    echo -e "\nPatching $DF (Debian base image instead of Alpine)"
    awk '{
        # Replace Alpine base image with Debian (avoids TLS cert issues in corporate networks)
        if (/jre-alpine/) {
            sub(/jre-alpine/, "jre")
            print
            next
        }
        # Skip the apk block entirely (Debian has bash built-in)
        if (/^RUN apk/) {
            skip = 1
            next
        }
        if (skip && /^[[:space:]]*add bash/) {
            skip = 0
            next
        }
        print
    }' "$DF" > "${DF}.tmp" && mv "${DF}.tmp" "$DF"
done
DOCKERFILES_PATCHED=1

# Build Docker image
echo -e "\nBuilding Docker image - apacheignite/ignite:$COMMIT_HASH"
cd "$TARGET_DIR"
docker build . -t apacheignite/ignite:"$COMMIT_HASH"

# Check success of previous command
if [ $? -ne 0 ]; then
    echo -e "\nError during Docker image build"
    exit 1
fi

# Return to project root directory
cd "$PROJECT_ROOT"

echo -e "\nDocker image built successfully - apacheignite/ignite:$COMMIT_HASH"