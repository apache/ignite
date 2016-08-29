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
 * Scalar function escape sequence parser tests.
 */
public class OdbcEscapeSequenceSelfTest extends GridCommonAbstractTest {
    /**
     * Test simple cases.
     */
    public void testSimple() {
        check(
            "select * from table;",
            "select * from table;"
        );

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
    }

    /**
     * Test escape sequence series.
     */
    public void testSimpleFunction() throws Exception {
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
     * Test non-closed escape sequence.
     */
    public void testFailedOnInvalidSequence1() {
        checkFail("select {fn func1(field1, {fn func2(field2), field3)} from SomeTable;");
    }

    /**
     * Test closing undeclared escape sequence.
     */
    public void testFailedOnClosingNotOpenedSequence() {
        checkFail("select {fn func1(field1, func2(field2)}, field3)} from SomeTable;");
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
