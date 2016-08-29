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

package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.processors.odbc.escape.OdbcEscapeUtils;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import java.util.concurrent.Callable;

/**
 * Escape sequence parser tests.
 */
public class OdbcEscapeSequenceSelfTest extends GridCommonAbstractTest {
    /**
     * Test simple cases.
     */
    public void testTrivial() {
        check(
            "select * from table;",
            "select * from table;"
        );
    }

    /**
     * Test escape sequence series.
     */
    public void testSimpleFunction() throws Exception {
        check(
            "test()",
            "{fn test()}"
        );

        check(
            "select test()",
            "select {fn test()}"
        );

        check(
            "select test() from table;",
            "select {fn test()} from table;"
        );

        check(
            "func(field1) func(field2)",
            "{fn func(field1)} {fn func(field2)}"
        );

        check(
            "select func(field1), func(field2)",
            "select {fn func(field1)}, {fn func(field2)}"
        );

        check(
            "select func(field1), func(field2) from table;",
            "select {fn func(field1)}, {fn func(field2)} from table;"
        );
    }

    /**
     * Test simple nested escape sequences. Depth = 2.
     */
    public void testNestedFunction() throws Exception {
        check(
            "func1(field1, func2(field2))",
            "{fn func1(field1, {fn func2(field2)})}"
        );

        check(
            "select func1(field1, func2(field2))",
            "select {fn func1(field1, {fn func2(field2)})}"
        );

        check(
            "select func1(field1, func2(field2), field3) from SomeTable;",
            "select {fn func1(field1, {fn func2(field2)}, field3)} from SomeTable;"
        );
    }

    /**
     * Test nested escape sequences. Depth > 2.
     */
    public void testDeepNestedFunction() {
        check(
            "func1(func2(func3(field1)))",
            "{fn func1({fn func2({fn func3(field1)})})}"
        );

        check(
            "func1(func2(func3(func4(field1))))",
            "{fn func1({fn func2({fn func3({fn func4(field1)})})})}"
        );

        check(
            "select func1(field1, func2(func3(field2), field3))",
            "select {fn func1(field1, {fn func2({fn func3(field2)}, field3)})}"
        );

        check(
            "select func1(field1, func2(func3(field2), field3)) from SomeTable;",
            "select {fn func1(field1, {fn func2({fn func3(field2)}, field3)})} from SomeTable;"
        );
    }

    /**
     * Test series of nested escape sequences.
     */
    public void testNestedFunctionMixed() {
        check(
            "func1(func2(field1), func3(field2))",
            "{fn func1({fn func2(field1)}, {fn func3(field2)})}"
        );

        check(
            "select func1(func2(field1), func3(field2)) from table;",
            "select {fn func1({fn func2(field1)}, {fn func3(field2)})} from table;"
        );

        check(
            "func1(func2(func3(field1))) func1(func2(field2))",
            "{fn func1({fn func2({fn func3(field1)})})} {fn func1({fn func2(field2)})}"
        );
    }

    /**
     * Test invalid escape sequence.
     */
    public void testFailedOnInvalidFunctionSequence() {
        checkFail("{fnfunc1()}");

        checkFail("select {fn func1(field1, {fn func2(field2), field3)} from SomeTable;");

        checkFail("select {fn func1(field1, fn func2(field2)}, field3)} from SomeTable;");
    }

    /**
     * Test escape sequences with additional whitespace characters
     */
    public void testFunctionEscapeSequenceWithWhitespaces() throws Exception {
        check("func1()", "{ fn func1()}");

        check("func1()", "{    fn  func1()}");

        check("func1()", "{ \n fn\nfunc1()}");

        checkFail("{ \n func1()}");
    }

    /**
     * Test guid escape sequences
     */
    public void testGuidEscapeSequence() {
        check(
            "'12345678-9abc-def0-1234-123456789abc'",
            "{guid '12345678-9abc-def0-1234-123456789abc'}"
        );

        check(
            "select '12345678-9abc-def0-1234-123456789abc' from SomeTable;",
            "select {guid '12345678-9abc-def0-1234-123456789abc'} from SomeTable;"
        );

        check(
            "select '12345678-9abc-def0-1234-123456789abc'",
            "select {guid '12345678-9abc-def0-1234-123456789abc'}"
        );
    }

    /**
     * Test invalid escape sequence.
     */
    public void testFailedOnInvalidGuidSequence() {
        checkFail("select {guid'12345678-9abc-def0-1234-123456789abc'}");

        checkFail("select {guid 12345678-9abc-def0-1234-123456789abc'}");

        checkFail("select {guid '12345678-9abc-def0-1234-123456789abc}");

        checkFail("select {guid '12345678-9abc-def0-1234-123456789abc' from SomeTable;");

        checkFail("select guid '12345678-9abc-def0-1234-123456789abc'} from SomeTable;");

        checkFail("select {guid '1234567-1234-1234-1234-123456789abc'}");

        checkFail("select {guid '1234567-8123-4123-4123-4123456789abc'}");

        checkFail("select {guid '12345678-9abc-defg-1234-123456789abc'}");

        checkFail("select {guid '12345678-12345678-1234-1234-1234-123456789abc'}");

        checkFail("select {guid '12345678-1234-1234-1234-123456789abcdef'}");
    }

    /**
     * Test escape sequences with additional whitespace characters
     */
    public void testGuidEscapeSequenceWithWhitespaces() throws Exception {
        check(
            "'12345678-9abc-def0-1234-123456789abc'",
            "{ guid '12345678-9abc-def0-1234-123456789abc'}"
        );

        check(
            "'12345678-9abc-def0-1234-123456789abc'",
            "{    guid  '12345678-9abc-def0-1234-123456789abc'}"
        );

        check(
            "'12345678-9abc-def0-1234-123456789abc'",
            "{  \n guid\n'12345678-9abc-def0-1234-123456789abc'}"
        );
    }

    /**
     * Test date escape sequences
     */
    public void testDateEscapeSequence() throws Exception {
        check(
            "'2016-08-26'",
            "{d '2016-08-26'}"
        );

        check(
            "select '2016-08-26'",
            "select {d '2016-08-26'}"
        );

        check(
            "select '2016-08-26' from table;",
            "select {d '2016-08-26'} from table;"
        );
    }

    /**
     * Test date escape sequences with additional whitespace characters
     */
    public void testDateEscapeSequenceWithWhitespaces() throws Exception {
        check("'2016-08-26'", "{ d '2016-08-26'}");

        check("'2016-08-26'", "{   d  '2016-08-26'}");

        check("'2016-08-26'", "{ \n d\n'2016-08-26'}");
    }

    /**
     * Test invalid escape sequence.
     */
    public void testFailedOnInvalidDateSequence() {
        checkFail("{d'2016-08-26'}");

        checkFail("{d 2016-08-26'}");

        checkFail("{d '2016-08-26}");

        checkFail("{d '16-08-26'}");

        checkFail("{d '2016/08/02'}");

        checkFail("select {d '2016-08-26' from table;");

        checkFail("select {}d '2016-08-26'} from table;");
    }

    /**
     * Test date escape sequences
     */
    public void testTimeEscapeSequence() throws Exception {
        check("'13:15:08'", "{t '13:15:08'}");

        check("select '13:15:08'", "select {t '13:15:08'}");

        check("select '13:15:08' from table;", "select {t '13:15:08'} from table;"
        );
    }

    /**
     * Test date escape sequences with additional whitespace characters
     */
    public void testTimeEscapeSequenceWithWhitespaces() throws Exception {
        check("'13:15:08'", "{ t '13:15:08'}");

        check("'13:15:08'", "{   t  '13:15:08'}");

        check("'13:15:08'", "{ \n t\n'13:15:08'}");
    }

    /**
     * Test invalid escape sequence.
     */
    public void testFailedOnInvalidTimeSequence() {
        checkFail("{t'13:15:08'}");

        checkFail("{t 13:15:08'}");

        checkFail("{t '13:15:08}");

        checkFail("{t '13 15:08'}");

        checkFail("{t '3:15:08'}");

        checkFail("select {t '13:15:08' from table;");

        checkFail("select {}t '13:15:08'} from table;");
    }

    /**
     * Test timestamp escape sequences
     */
    public void testTimestampEscapeSequence() throws Exception {
        check(
            "'2016-08-26 13:15:08'",
            "{ts '2016-08-26 13:15:08'}"
        );

        check(
            "'2016-08-26 13:15:08.123456'",
            "{ts '2016-08-26 13:15:08.123456'}"
        );

        check(
            "select '2016-08-26 13:15:08'",
            "select {ts '2016-08-26 13:15:08'}"
        );

        check(
            "select '2016-08-26 13:15:08' from table;",
            "select {ts '2016-08-26 13:15:08'} from table;"
        );
    }

    /**
     * Test timestamp escape sequences with additional whitespace characters
     */
    public void testTimestampEscapeSequenceWithWhitespaces() throws Exception {
        check("'2016-08-26 13:15:08'",
            "{ ts '2016-08-26 13:15:08'}"
        );

        check("'2016-08-26 13:15:08'",
            "{   ts  '2016-08-26 13:15:08'}"
        );

        check("'2016-08-26 13:15:08'",
            "{ \n ts\n'2016-08-26 13:15:08'}"
        );
    }

    /**
     * Test invalid escape sequence.
     */
    public void testFailedOnInvalidTimestampSequence() {
        checkFail("{ts '2016-08-26 13:15:08,12345'}");

        checkFail("{ts'2016-08-26 13:15:08'}");

        checkFail("{ts 2016-08-26 13:15:08'}");

        checkFail("{ts '2016-08-26 13:15:08}");

        checkFail("{ts '16-08-26 13:15:08'}");

        checkFail("{ts '2016-08-26 3:25:08'}");

        checkFail("{ts '2016-08 26 03:25:08'}");

        checkFail("{ts '2016-08-26 03 25:08'}");

        checkFail("{t s '2016-08-26 13:15:08''}");

        checkFail("select {ts '2016-08-26 13:15:08' from table;");

        checkFail("select {}ts '2016-08-26 13:15:08'} from table;");
    }

    /**
     * Check parsing logic.
     *
     * @param exp Expected result.
     * @param qry SQL query text.
     */
    private void check(String exp, String qry) {
        String actualRes = OdbcEscapeUtils.parse(qry);

        assertEquals(exp, actualRes);
    }

    /**
     * Check that query parsing fails.
     *
     * @param qry Query.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    private void checkFail(final String qry) {
        GridTestUtils.assertThrows(null, new Callable<Void>() {
            @Override public Void call() throws Exception {
                OdbcEscapeUtils.parse(qry);

                fail("Parsing should fail: " + qry);

                return null;
            }
        }, IgniteException.class, null);
    }
}
