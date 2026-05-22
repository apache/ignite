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

# Default to master if no base branch is specified as an argument
TARGET_BASE="${1:-master}"

echo "Calculating diff baseline against branch: $TARGET_BASE..."

# Automatically resolve SHA values simulating GitHub's environment
export BASE_SHA=$(git merge-base "$TARGET_BASE" HEAD 2>/dev/null || { echo "❌ Error: Branch '$TARGET_BASE' not found."; exit 1; })
export HEAD_SHA=$(git rev-parse HEAD)
export GITHUB_OUTPUT=/dev/null
export HITS_FILE=/tmp/protected-hits.txt

# Run the core validation engine
chmod +x ./check-protected-classes.sh
./check-protected-classes.sh

# Evaluate the outputs
if [ -s "$HITS_FILE" ]; then
  echo -e "\nFLAG TRIPPED: The following protected files were altered:"
  echo "--------------------------------------------------------"
  cat "$HITS_FILE"
  echo "--------------------------------------------------------"
  exit 1
else
  echo -e "\nSUCCESS: No protected class modifications detected."
  exit 0
fi
