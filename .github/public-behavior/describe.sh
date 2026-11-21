#!/usr/bin/env bash
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

# Builds the source tree in the current directory and prints its public declarations.
# Usage: cd <source tree> && bash <path to this script> > declarations.txt

set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)

# test-compile resolves test-jar dependencies to reactor directories; -Dmaven.test.skip=true skips compiling the tests.
./mvnw -B -ntp test-compile dependency:build-classpath -Dmdep.outputFile=target/classpath.txt -Dmdep.includeScope=compile \
    -Dmaven.test.skip=true -Plgpl -T 1C >&2

cp=$( { find modules -path '*/target/classpath.txt' -exec cat {} + | tr ':' '\n'; find "$PWD/modules" -path '*/target/classes' -type d; } \
    | grep . | sort -u | paste -sd: -)

. bin/include/jvmdefaults.sh

read -ra opts <<< "$(getJavaSpecificOpts 17 "")"

java "${opts[@]}" -cp "$cp" "$here/PublicDeclarations.java" .
