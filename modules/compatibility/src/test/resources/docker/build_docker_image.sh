#!/bin/bash

# Get the absolute path to the project root directory
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../../../../" && pwd)"

# Check that commit hash is provided
if [ $# -ne 1 ]; then
    echo "Usage: $0 <commit_hash>"
    exit 1
fi

COMMIT_HASH=$1

# Change to the project root directory
cd "$PROJECT_ROOT" || exit 1

# 1) Perform git checkout to the specified commit
echo -e "\nPerforming git checkout to commit: $COMMIT_HASH"
git checkout "$COMMIT_HASH"

# 2) Build the project
echo -e "\nBuilding the project: ./mvnw clean install -Pall-java,licenses -DskipTests"
./mvnw clean install -Pall-java,licenses -DskipTests

# Check success of previous command
if [ $? -ne 0 ]; then
    echo -e "\nError during project build"
    exit 1
fi

# 3) Initialize the release
echo -e "\nInitializing release: ./mvnw initialize -Prelease"
./mvnw initialize -Prelease

# Check success of previous command
if [ $? -ne 0 ]; then
    echo -e "\nError during release initialization"
    exit 1
fi

# 4) Copy and unpack the release archive
echo -e "\nCopying and unpacking the release archive"
TARGET_DIR="$PROJECT_ROOT/deliveries/docker/apache-ignite/x86_64"
SOURCE_ARCHIVE="$PROJECT_ROOT/target/bin/apache-ignite-*.zip"

# Check if archive exists
if [ ! -f $(echo $SOURCE_ARCHIVE) ]; then
    echo -e "\nArchive not found: $SOURCE_ARCHIVE"
    exit 1
fi

# Copy archive to target directory
cp $(echo $SOURCE_ARCHIVE) "$TARGET_DIR/"

# Unpack the archive
cd "$TARGET_DIR"
unzip "$(basename $(echo $SOURCE_ARCHIVE))"

# Return to project root directory
cd "$PROJECT_ROOT"

# 5) Copy the startup script
echo -e "\nCopying startup script"
cp "$PROJECT_ROOT/deliveries/docker/apache-ignite/run.sh" "$TARGET_DIR/"

# 6) Copy compatibility tests jar file to libs directory
echo -e "\nCopying compatibility jar to libs directory"
# Find the tests jar file in the target directory
COMPATIBILITY_JAR=""
for file in "$PROJECT_ROOT/modules/compatibility/target"/ignite-compatibility-*-tests.jar; do
    if [ -f "$file" ]; then
        COMPATIBILITY_JAR="$file"
        break
    fi
done

if [ -n "$COMPATIBILITY_JAR" ] && [ -f "$COMPATIBILITY_JAR" ]; then
    # Get the actual jar file name
    ACTUAL_JAR=$(basename "$COMPATIBILITY_JAR")
    # Find the actual ignite directory name after unpacking
    IGNITE_DIR=$(ls -d "$TARGET_DIR"/apache-ignite-* 2>/dev/null | head -n 1)
    if [ -n "$IGNITE_DIR" ]; then
        # Copy to libs directory inside unpacked distribution
        cp "$COMPATIBILITY_JAR" "$IGNITE_DIR/libs/"
        echo -e "Compatibility tests jar copied successfully to libs directory"
    else
        echo -e "\nIgnite directory not found"
        exit 1
    fi
else
    echo -e "\nCompatibility tests jar not found"
    exit 1
fi

# 7) Build Docker image
echo -e "\nBuilding Docker image - apacheignite/ignite:$COMMIT_HASH"
cd "$TARGET_DIR"
docker build . -t apacheignite/ignite:"$COMMIT_HASH"

# Return to project root directory
cd "$PROJECT_ROOT"

echo -e "\nDocker image built successfully - apacheignite/ignite:$COMMIT_HASH"