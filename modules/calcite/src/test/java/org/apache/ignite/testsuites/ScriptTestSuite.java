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
import org.junit.runner.RunWith;

/**
 * Test suite to run SQL test scripts.
 *
 * By default, only "*.test" and "*.test_slow" scripts are run.
 * Other files are ignored.
 *
 * Use {@link ScriptRunnerTestsEnvironment#regex()} property to specify regular expression for filter
 * script path to debug run. In this case the file suffix will be ignored.
 * e.g. regex = "test_aggr_string.test"
 *
 * Use other properties of the {@link ScriptRunnerTestsEnvironment} to setup cluster and test environment.<br><br>
 *
 * All test files consist of appropriate collection of queries.
 * A query record begins with a line of the following form:
 * query <type-string> <sort-mode> <label> <br><br>
 *
 * The SQL for the query is found on second an subsequent lines of the record up to first line of the form "----"
 * or until the end of the record. Lines following the "----" are expected results of the query, one value per line.
 * If the "----" and/or the results are omitted, then the query is expected to return an empty set.
 * The "----" and results are also omitted from prototype scripts and are always ignored when the sqllogictest program
 * is operating in completion mode. Another way of thinking about completion mode is that it copies the script from
 * input to output, replacing all "----" lines and subsequent result values with the actual results from running the
 * query.<br><br>
 *
 * The <type-string> argument to the query statement is a short string that specifies the number of result columns and
 * the expected datatype of each result column. There is one character in the <type-string> for each result column.
 * The characters codes are "T" for a text result, "I" for an integer result, and "R" for a floating-point
 * result.<br><br>
 *
 * The <sort-mode> argument is optional. If included, it must be one of "nosort", "rowsort", or "valuesort".
 * The default is "nosort". In nosort mode, the results appear in exactly the order in which they were received from
 * the database engine. The nosort mode should only be used on queries that have an ORDER BY clause or which only have
 * a single row of result, since otherwise the order of results is undefined and might vary from one database engine
 * to another. The "rowsort" mode gathers all output from the database engine then sorts it by rows on the client side.
 * Sort comparisons use strcmp() on the rendered ASCII text representation of the values. Hence, "9" sorts after "10",
 * not before. The "valuesort" mode works like rowsort except that it does not honor row groupings. Each individual
 * result value is sorted on its own.<br><br>
 *
 * The <label> argument is also optional. If included, sqllogictest stores a hash of the results of this query under
 * the given label. If the label is reused, then sqllogictest verifies that the results are the same.
 * This can be used to verify that two or more queries in the same test script that are logically equivalent
 * always generate the same output.<br><br>
 *
 * In the results section, integer values are rendered as if by printf("%d"). Floating point values are rendered as
 * if by printf("%.3f"). NULL values are rendered as "NULL". Empty strings are rendered as "(empty)". Within non-empty
 * strings, all control characters and unprintable characters are rendered as "@".<br><br>
 *
 * @see <a href="https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki">Extended format documentation.</a></a>
 *
 */
@RunWith(ScriptTestRunner.class)
@ScriptRunnerTestsEnvironment(scriptsRoot = "modules/calcite/src/test/sql/types/collections", timeout = 180000)
public class ScriptTestSuite {
}
