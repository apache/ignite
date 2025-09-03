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

#
# Checks wheter SQL Calcite operators can be executed and no missed dependencies.
#
set -ex

IGNITE_HOME=$PWD/target/release-package-apache-ignite

ls $PWD/modules/calcite/target

USER_LIBS="$IGNITE_HOME/libs/*"

for lib in "$IGNITE_HOME"/libs/ignite-*; do
    USER_LIBS="$USER_LIBS:$lib/*"
done

for lib in ignite-log4j2 ignite-calcite; do
    USER_LIBS="$USER_LIBS:$IGNITE_HOME/libs/optional/$lib/*"
done

# Add *-tests.jar to classpath to include the test to run.
for testlib in core calcite; do
    USER_LIBS="$USER_LIBS:$PWD/modules/$testlib/target/*"
done

M2_HOME=~/.m2/repository

JUNIT=$M2_HOME/junit/junit/4.13.2/junit-4.13.2.jar
HAMCREST=$M2_HOME/org/hamcrest/hamcrest/2.2/hamcrest-2.2.jar

# Runs a test of Calcite SQL operators that require transitive dependencies.
# If this test fails, than missed dependency must be added to modules/calcite/pom.xml.
java -cp $USER_LIBS:$JUNIT:$HAMCREST \
  org.junit.runner.JUnitCore \
  org.apache.ignite.internal.processors.query.calcite.integration.StdSqlOperatorsTest
