#!/usr/bin/env bash

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

set -euo pipefail

# Inputs provided via environment variables or defaults
BASE_SHA="${BASE_SHA}"
HEAD_SHA="${HEAD_SHA}"
ANNOTATION="${ANNOTATION:-org.apache.ignite.internal.Order}"
HITS_FILE="${HITS_FILE:-/tmp/protected-hits.txt}"
GITHUB_OUTPUT="${GITHUB_OUTPUT:-/dev/null}"

# Automatically locate and navigate to the repository root directory
REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

touch "$HITS_FILE"

# 1. Check added/deleted files for the annotation in the diff context
git diff --name-only --no-renames --diff-filter=AD "$BASE_SHA"..."$HEAD_SHA" -- '*.java' | while read -r file; do
  [ -z "$file" ] && continue
  if git diff "$BASE_SHA"..."$HEAD_SHA" -- "$file" | grep -q "$ANNOTATION"; then
    echo "$file" >> "$HITS_FILE"
  fi
done

# 2. Check modified files instantly by searching the file contents at BASE_SHA
MODIFIED_FILES=$(git diff --name-only --no-renames --diff-filter=M "$BASE_SHA"..."$HEAD_SHA" -- '*.java')

if [ -n "$MODIFIED_FILES" ]; then
  git grep -l "$ANNOTATION" "$BASE_SHA" -- $MODIFIED_FILES >> "$HITS_FILE" 2>/dev/null || true
fi

# Clean up and produce Outputs
if [ -s "$HITS_FILE" ]; then
  sed -i "s/^${BASE_SHA}://g" "$HITS_FILE"
  sort -u -o "$HITS_FILE" "$HITS_FILE"
  echo "affected=true" >> "$GITHUB_OUTPUT"
fi
