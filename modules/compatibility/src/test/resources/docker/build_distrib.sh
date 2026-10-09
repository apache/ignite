#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Prepares target libs for DOCKER upgrade mode:
#   1. Builds the distribution ZIP if missing.
#   2. Extracts the ZIP into target/bin/.
#   3. Creates a symlink project/target/ignite-target-libs -> project/target/bin/apache-ignite-*-bin/libs.
#
# Arguments:
#   $1 - path to project root (where mvnw is located)

set -e

PROJECT_ROOT="$1"
LIBS_SYMLINK="${PROJECT_ROOT}/target/ignite-target-libs"

# If symlink already points to a valid directory — nothing to do
if [ -L "$LIBS_SYMLINK" ] && [ -d "$LIBS_SYMLINK" ]; then
    echo "[compatibility-docker] Target libs symlink exists at ${LIBS_SYMLINK} — skipping"
    exit 0
fi

DIST_ZIP_DIR="${PROJECT_ROOT}/target/bin"

# Build distribution if ZIP archive is missing
if [ ! -d "$DIST_ZIP_DIR" ] || [ -z "$(ls "$DIST_ZIP_DIR"/apache-ignite-*-bin.zip 2>/dev/null)" ]; then
    echo "[compatibility-docker] Distribution ZIP not found. Building project and distribution..."
    cd "$PROJECT_ROOT"
    ./mvnw clean install -T1C -Pall-java -DskipTests
    ./mvnw initialize -Prelease
fi

# Find the distribution ZIP
DIST_ZIP=$(ls -1 "$DIST_ZIP_DIR"/apache-ignite-*-bin.zip 2>/dev/null | head -1)

if [ -z "$DIST_ZIP" ]; then
    echo "[compatibility-docker] ERROR: Distribution ZIP not found in ${DIST_ZIP_DIR}" >&2
    exit 1
fi

# Extract ZIP into target/bin/
echo "[compatibility-docker] Extracting ${DIST_ZIP} into ${DIST_ZIP_DIR}..."
unzip -q -o "$DIST_ZIP" -d "$DIST_ZIP_DIR"

# Find the unpacked libs directory (structure: apache-ignite-*/libs/)
LIBS_DIR=$(find "$DIST_ZIP_DIR" -mindepth 2 -maxdepth 2 -type d -name libs | head -1)

if [ -z "$LIBS_DIR" ]; then
    echo "[compatibility-docker] ERROR: libs/ directory not found after extraction" >&2
    exit 1
fi

# Remove stale symlink if it exists
rm -f "$LIBS_SYMLINK"

# Create symlink: target/ignite-target-libs -> target/bin/apache-ignite-*-bin/libs
ln -s "$LIBS_DIR" "$LIBS_SYMLINK"

echo "[compatibility-docker] Done. Symlink: ${LIBS_SYMLINK} -> ${LIBS_DIR}"
