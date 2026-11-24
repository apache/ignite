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

# classpath.txt has no line break at the end, so each file is split on its own.
cp=$( { find modules -path '*/target/classpath.txt' -exec sh -c 'for f; do tr ":" "\n" < "$f"; echo; done' _ {} +;
    find "$PWD/modules" -path '*/target/classes' -type d; } | grep . | sort -u | paste -sd: -)

. bin/include/jvmdefaults.sh

read -ra opts <<< "$(getJavaSpecificOpts 17 "")"

classes=$(mktemp -d)
trap 'rm -rf "$classes"' EXIT

javac -proc:none -nowarn -d "$classes" -cp "$cp" "$here"/src/*.java "$here"/src/framework/*.java "$here"/src/collectors/*.java >&2

# The API of another platform is described only when the workflow asks for it, and then for both builds: a PR that does not
# change the platform does not pay for describing it.
platforms="$classes/platforms"
mkdir "$platforms"

if [ "${DESCRIBE_CPP:-false}" = true ]; then
    python3 "$here/platforms/cpp.py" > "$platforms/cpp.tsv"
fi

if [ "${DESCRIBE_DOTNET:-false}" = true ]; then
    # The describer is built outside the tree, so its build files do not land in the checkout.
    cp -R "$here/platforms/dotnet" "$classes/dotnet-src"
    dotnet build "$classes/dotnet-src/Describe.csproj" -c Release -nologo -v q -o "$classes/dotnet" >&2
    dotnet "$classes/dotnet/Describe.dll" > "$platforms/dotnet.tsv"
fi

PUBLIC_BEHAVIOR_PLATFORMS="$platforms" java "${opts[@]}" -cp "$classes:$cp" PublicDeclarations .
