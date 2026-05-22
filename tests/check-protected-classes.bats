#!/usr/bin/env bats

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

setup() {
  # Dynamically calculate the repository root folder path
  DIR="$( cd "$( dirname "$BATS_TEST_FILENAME" )" >/dev/null 2>&1 && pwd )"
  export SCRIPT_PATH="$DIR/../scripts/check-protected-classes.sh"

  export BASE_SHA="baseline123"
  export HEAD_SHA="feature456"
  export HITS_FILE="${BATS_TMPDIR}/hits.txt"
  export GITHUB_OUTPUT="${BATS_TMPDIR}/output.env"

  rm -f "$HITS_FILE" "$GITHUB_OUTPUT"
  touch "$GITHUB_OUTPUT"
}

# Mock Git command execution paths
git() {
  if [[ "$*" == *"diff --name-only"* && *"diff-filter=AD"* ]]; then
    echo "${MOCK_AD_FILES:-}"
  elif [[ "$*" == *"diff --name-only"* && *"diff-filter=M"* ]]; then
    echo "${MOCK_M_FILES:-}"
  elif [[ "$*" == *"diff"* && *"${MOCK_MATCHING_FILE:-}"* ]]; then
    echo "+ @org.apache.ignite.internal.Order"
  elif [[ "$*" == *"grep"* ]]; then
    if [ -n "${MOCK_GREP_MATCHES:-}" ]; then
      echo "${BASE_SHA}:${MOCK_GREP_MATCHES}"
      return 0
    fi
    return 1
  else
    command git "$@"
  fi
}

# Helper function to execute our script within the mocked environment context
run_script() {
  # Sourcing it instead of calling 'bash script.sh' keeps our mocked git() function active
  run source "$SCRIPT_PATH"
}

@test "Scenario 1: No changes should yield zero alerts" {
  export MOCK_AD_FILES=""
  export MOCK_M_FILES=""

  run_script

  [ "$status" -eq 0 ]
  [ ! -s "$HITS_FILE" ]
  ! grep -q "affected=true" "$GITHUB_OUTPUT"
}

@test "Scenario 2: Newly added file with target annotation triggers hit" {
  export MOCK_AD_FILES="src/main/java/NewProtectedClass.java"
  export MOCK_MATCHING_FILE="NewProtectedClass.java"

  run_script

  [ "$status" -eq 0 ]
  grep -q "src/main/java/NewProtectedClass.java" "$HITS_FILE"
  grep -q "affected=true" "$GITHUB_OUTPUT"
}

@test "Scenario 3: Modified file containing annotation triggers hit" {
  export MOCK_M_FILES="src/main/java/ModifiedClass.java"
  export MOCK_GREP_MATCHES="src/main/java/ModifiedClass.java"

  run_script

  [ "$status" -eq 0 ]
  grep -q "src/main/java/ModifiedClass.java" "$HITS_FILE"
  grep -q "affected=true" "$GITHUB_OUTPUT"
}
