/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testsuites;

import org.apache.ignite.internal.processors.query.calcite.logical.ScriptRunnerTestsEnvironment;
import org.apache.ignite.internal.processors.query.calcite.logical.ScriptTestRunner;
import org.junit.Ignore;
import org.junit.runner.RunWith;

/**
 * Test suite to run SQL test scripts.
 *
 * By default only "*.test" and "*.test_slow" scripts are run.
 * Other files are ignored.
 *
 * Use {@link ScriptRunnerTestsEnvironment#regex()} property to specify regular expression for filter
 * script path to debug run. In this case the file suffix will be ignored.
 * e.g. regex = "test_aggr_string.test"
 *
 * Use other properties of the {@link ScriptRunnerTestsEnvironment} to setup cluster and test environment.
 */
@RunWith(ScriptTestRunner.class)
@ScriptRunnerTestsEnvironment(scriptsRoot = "src/test/sql", regex = "test_aggregate_types.test")
public class ScriptTestSuite {
}
