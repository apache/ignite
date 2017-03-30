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
     * Test escape sequence for explicit data type conversion
     */
    public void testConvertFunction() throws Exception {
        check(
         "CONVERT ( CURDATE(), CHAR )",
         "{ fn CONVERT ( { fn CURDATE() }, SQL_CHAR ) }"
        );

        check(
         "conVerT ( some_expression('one', 'two') , DECIMAL ( 5 , 2 ) )",
         "{ fn conVerT ( some_expression('one', 'two') , SQL_DECIMAL ( 5 , 2 ) ) }"
        );

        check(
         "convert(field,CHAR)",
         "{fn convert(field,sql_char)}"
        );

        check(
         "convert(field,BIGINT)",
         "{fn convert(field,sql_bigint)}"
        );

        check(
         "convert(field,BINARY)",
         "{fn convert(field,sql_binary)}" // also sql_varbinary,sql_longvarbinary
        );

        check(
         "convert(field,BIT)",
         "{fn convert(field,sql_bit)}"
        );

        check(
         "convert(field,CHAR(100))",
         "{fn convert(field,sql_char(100))}"
        );

        check(
         "convert(field,DECIMAL(5,2))",
         "{fn convert(field,sql_decimal(5,2))}" // also sql_numeric
        );

        check(
         "convert(field,VARCHAR(100))",
         "{fn convert(field,sql_varchar(100))}" // also sql_longvarchar,sql_wchar,sql_wlongvarchar,sql_wvarchar
        );

        check(
         "convert(field,DOUBLE)",
         "{fn convert(field,sql_double)}" // also sql_float
        );

        check(
         "convert(field,REAL)",
         "{fn convert(field,sql_real)}"
        );

        check(
         "convert(field,UUID)",
         "{fn convert(field,sql_guid)}"
        );

        check(
         "convert(field,SMALLINT)",
         "{fn convert(field,sql_smallint)}"
        );

        check(
         "convert(field,INTEGER)",
         "{fn convert(field,sql_integer)}"
        );

        check(
         "convert(field,DATE)",
         "{fn convert(field,sql_date)}"
        );

        check(
         "convert(field,TIME)",
         "{fn convert(field,sql_time)}"
        );

        check(
         "convert(field,TIMESTAMP)",
         "{fn convert(field,sql_timestamp)}"
        );

        check(
         "convert(field,TINYINT)",
         "{fn convert(field,sql_tinyint)}"
        );

        //invalid odbc type
        checkFail("{fn convert(field,char)}");

        //no support for interval types
        checkFail("{fn convert(field,sql_interval_second)}");
        checkFail("{fn convert(field,sql_interval_minute)}");
        checkFail("{fn convert(field,sql_interval_hour)}");
        checkFail("{fn convert(field,sql_interval_day)}");
        checkFail("{fn convert(field,sql_interval_month)}");
        checkFail("{fn convert(field,sql_interval_year)}");
        checkFail("{fn convert(field,sql_interval_year_to_month)}");
        checkFail("{fn convert(field,sql_interval_hour_to_minute)}");
        checkFail("{fn convert(field,sql_interval_hour_to_second)}");
        checkFail("{fn convert(field,sql_interval_minute_to_second)}");
        checkFail("{fn convert(field,sql_interval_day_to_hour)}");
        checkFail("{fn convert(field,sql_interval_day_to_minute)}");
        checkFail("{fn convert(field,sql_interval_day_to_second)}");

        //failure: expected function name
        checkFail("{fn    }");

        //failure: expected function parameter list
        checkFail("{fn convert}");

        //failure: expected data type parameter for convert function
        checkFail("{fn convert ( justoneparam ) }");

        //failure: empty precision/scale
        checkFail("{fn convert ( justoneparam, sql_decimal( ) }");

        //failure: empty precision/scale
        checkFail("{fn convert ( justoneparam, sql_decimal(not_a_number) }");

        //failure: missing scale after comma
        checkFail("{fn convert ( justoneparam, sql_decimal(10,) }");
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
            "CAST('12345678-9abc-def0-1234-123456789abc' AS UUID)",
            "{guid '12345678-9abc-def0-1234-123456789abc'}"
        );

        check(
            "select CAST('12345678-9abc-def0-1234-123456789abc' AS UUID) from SomeTable;",
            "select {guid '12345678-9abc-def0-1234-123456789abc'} from SomeTable;"
        );

        check(
            "select CAST('12345678-9abc-def0-1234-123456789abc' AS UUID)",
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
            "CAST('12345678-9abc-def0-1234-123456789abc' AS UUID)",
            "{ guid '12345678-9abc-def0-1234-123456789abc'}"
        );

        check(
            "CAST('12345678-9abc-def0-1234-123456789abc' AS UUID)",
            "{    guid  '12345678-9abc-def0-1234-123456789abc'}"
        );

        check(
            "CAST('12345678-9abc-def0-1234-123456789abc' AS UUID)",
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
     * Test escape sequence series.
     */
    public void testOuterJoinFunction() throws Exception {
        check(
            "t OUTER JOIN t2 ON t.id=t2.id",
            "{oj t OUTER JOIN t2 ON t.id=t2.id}"
        );

        check(
            "select * from t OUTER JOIN t2 ON t.id=t2.id",
            "select * from {oj t OUTER JOIN t2 ON t.id=t2.id}"
        );

        check(
            "select * from t OUTER JOIN t2 ON t.id=t2.id ORDER BY t2.id",
            "select * from {oj t OUTER JOIN t2 ON t.id=t2.id} ORDER BY t2.id"
        );
    }

    /**
     * Test simple nested escape sequences. Depth = 2.
     */
    public void testNestedOuterJoin() throws Exception {
        check(
            "t OUTER JOIN (t2 OUTER JOIN t3 ON t2.id=t3.id) ON t.id=t2.id",
            "{oj t OUTER JOIN ({oj t2 OUTER JOIN t3 ON t2.id=t3.id}) ON t.id=t2.id}"
        );

        check(
            "select * from t OUTER JOIN (t2 OUTER JOIN t3 ON t2.id=t3.id) ON t.id=t2.id",
            "select * from {oj t OUTER JOIN ({oj  t2 OUTER JOIN t3 ON t2.id=t3.id}) ON t.id=t2.id}"
        );

        check(
            "select * from t OUTER JOIN (t2 OUTER JOIN t3 ON t2.id=t3.id) ON t.id=t2.id ORDER BY t2.id",
            "select * from {oj t OUTER JOIN ({oj t2 OUTER JOIN t3 ON t2.id=t3.id}) ON t.id=t2.id} ORDER BY t2.id"
        );
    }

    /**
     * Test nested escape sequences. Depth > 2.
     */
    public void testDeepNestedOuterJoin() {
        check(
            "t OUTER JOIN (t2 OUTER JOIN (t3 OUTER JOIN t4 ON t3.id=t4.id) ON t2.id=t3.id) ON t.id=t2.id",
            "{oj t OUTER JOIN ({oj t2 OUTER JOIN ({oj t3 OUTER JOIN t4 ON t3.id=t4.id}) ON t2.id=t3.id}) ON t.id=t2.id}"
        );

        check(
            "select * from " +
                "t OUTER JOIN (t2 OUTER JOIN (t3 OUTER JOIN t4 ON t3.id=t4.id) ON t2.id=t3.id) ON t.id=t2.id",
            "select * from " +
                "{oj t OUTER JOIN ({oj t2 OUTER JOIN ({oj t3 OUTER JOIN t4 ON t3.id=t4.id}) ON t2.id=t3.id})" +
                " ON t.id=t2.id}"
        );

        check(
            "select * from t OUTER JOIN (t2 OUTER JOIN (t3 OUTER JOIN t4 ON t3.id=t4.id) " +
                "ON t2.id=t3.id) ON t.id=t2.id ORDER BY t4.id",
            "select * from {oj t OUTER JOIN ({oj t2 OUTER JOIN ({oj t3 OUTER JOIN t4 ON t3.id=t4.id}) " +
                "ON t2.id=t3.id}) ON t.id=t2.id} ORDER BY t4.id"
        );
    }

    /**
     * Test invalid escape sequence.
     */
    public void testFailedOnInvalidOuterJoinSequence() {
        checkFail("{ojt OUTER JOIN t2 ON t.id=t2.id}");

        checkFail("select {oj t OUTER JOIN ({oj t2 OUTER JOIN t3 ON t2.id=t3.id) ON t.id=t2.id} from SomeTable;");

        checkFail("select oj t OUTER JOIN t2 ON t.id=t2.id} from SomeTable;");
    }

    /**
     * Test escape sequences with additional whitespace characters
     */
    public void testOuterJoinSequenceWithWhitespaces() throws Exception {
        check(
            "t OUTER JOIN t2 ON t.id=t2.id",
            "{ oj t OUTER JOIN t2 ON t.id=t2.id}"
        );

        check(
            "t OUTER JOIN t2 ON t.id=t2.id",
            "{    oj  t OUTER JOIN t2 ON t.id=t2.id}"
        );

        check(
            "t OUTER JOIN t2 ON t.id=t2.id",
            "  \n { oj\nt OUTER JOIN t2 ON t.id=t2.id}"
        );
    }

    /**
     * Test non-escape sequences.
     */
    public void testNonEscapeSequence() throws Exception {
        check("'{fn test()}'", "'{fn test()}'");

        check("select '{fn test()}'", "select '{fn test()}'");

        check(
            "select '{fn test()}' from table;",
            "select '{fn test()}' from table;"
        );

        check(
            "select test('arg')  from table;",
            "select {fn test('arg')}  from table;"
        );

        check(
            "select test('{fn func()}')  from table;",
            "select {fn test('{fn func()}')}  from table;"
        );

        check(
            "'{\\'some literal\\'}'",
            "'{\\'some literal\\'}'"
        );

        check(
            "select '{\\'some literal\\'}'",
            "select '{\\'some literal\\'}'"
        );

        check(
            "select '{\\'some literal\\'}' from table;",
            "select '{\\'some literal\\'}' from table;"
        );

        check(
            "select '{' + func() + '}' from table;",
            "select '{' + {fn func()} + '}' from table;"
        );

        // quoted two single quotes should be interpret as apostrophe
        check(
            "select '{''{fn test()}''}' from table;",
            "select '{''{fn test()}''}' from table;"
        );

        checkFail("'{fn test()}");

        checkFail("{fn func('arg)}");

        checkFail("{fn func(arg')}");
    }


    /**
     * Test escape sequence series.
     */
    public void testSimpleCallProc() throws Exception {
        check(
            "CALL test()",
            "{call test()}"
        );

        check(
            "select CALL test()",
            "select {call test()}"
        );

        check(
            "select CALL test() from table;",
            "select {call test()} from table;"
        );

        check(
            "CALL func(field1) CALL func(field2)",
            "{call func(field1)} {call func(field2)}"
        );

        check(
            "select CALL func(field1), CALL func(field2)",
            "select {call func(field1)}, {call func(field2)}"
        );

        check(
            "select CALL func(field1), CALL func(field2) from table;",
            "select {call func(field1)}, {call func(field2)} from table;"
        );
    }

    /**
     * Test simple nested escape sequences. Depth = 2.
     */
    public void testNestedCallProc() throws Exception {
        check(
            "CALL func1(field1, CALL func2(field2))",
            "{call func1(field1, {call func2(field2)})}"
        );

        check(
            "select CALL func1(field1, CALL func2(field2))",
            "select {call func1(field1, {call func2(field2)})}"
        );

        check(
            "select CALL func1(field1, CALL func2(field2), field3) from SomeTable;",
            "select {call func1(field1, {call func2(field2)}, field3)} from SomeTable;"
        );
    }

    /**
     * Test nested escape sequences. Depth > 2.
     */
    public void testDeepNestedCallProc() {
        check(
            "CALL func1(CALL func2(CALL func3(field1)))",
            "{call func1({call func2({call func3(field1)})})}"
        );

        check(
            "CALL func1(CALL func2(CALL func3(CALL func4(field1))))",
            "{call func1({call func2({call func3({call func4(field1)})})})}"
        );

        check(
            "select CALL func1(field1, CALL func2(CALL func3(field2), field3))",
            "select {call func1(field1, {call func2({call func3(field2)}, field3)})}"
        );

        check(
            "select CALL func1(field1, CALL func2(CALL func3(field2), field3)) from SomeTable;",
            "select {call func1(field1, {call func2({call func3(field2)}, field3)})} from SomeTable;"
        );
    }

    /**
     * Test series of nested escape sequences.
     */
    public void testNestedCallProcMixed() {
        check(
            "CALL func1(CALL func2(field1), CALL func3(field2))",
            "{call func1({call func2(field1)}, {call func3(field2)})}"
        );

        check(
            "select CALL func1(CALL func2(field1), CALL func3(field2)) from table;",
            "select {call func1({call func2(field1)}, {call func3(field2)})} from table;"
        );

        check(
            "CALL func1(CALL func2(CALL func3(field1))) CALL func1(CALL func2(field2))",
            "{call func1({call func2({call func3(field1)})})} {call func1({call func2(field2)})}"
        );
    }

    /**
     * Test invalid escape sequence.
     */
    public void testFailedOnInvalidCallProcSequence() {
        checkFail("{callfunc1()}");

        checkFail("select {call func1(field1, {call func2(field2), field3)} from SomeTable;");

        checkFail("select {call func1(field1, call func2(field2)}, field3)} from SomeTable;");
    }

    /**
     * Test escape sequences with additional whitespace characters
     */
    public void testCallProcEscapeSequenceWithWhitespaces() throws Exception {
        check("CALL func1()", "{ call func1()}");

        check("CALL func1()", "{    call  func1()}");

        check("CALL func1()", "{ \n call\nfunc1()}");

        checkFail("{ \n func1()}");
    }

    /**
     * Test escape sequence series.
     */
    public void testLikeEscapeSequence() throws Exception {
        check(
            "ESCAPE '\\'",
            "{'\\'}"
        );

        check(
            "ESCAPE '\\'",
            "{escape '\\'}"
        );

        check(
            "ESCAPE ''",
            "{''}"
        );

        check(
            "ESCAPE ''",
            "{escape ''}"
        );

        check(
            "select * from t where value LIKE '\\%AAA%' ESCAPE '\\'",
            "select * from t where value LIKE '\\%AAA%' {'\\'}"
        );

        check(
            "select * from t where value LIKE '\\%AAA%' ESCAPE '\\'",
            "select * from t where value LIKE '\\%AAA%' {escape '\\'}"
        );

        check(
            "select * from t where value LIKE '\\%AAA%' ESCAPE '\\' ORDER BY id;",
            "select * from t where value LIKE '\\%AAA%' {'\\'} ORDER BY id;"
        );

        check(
            "select * from t where value LIKE '\\%AAA%' ESCAPE '\\' ORDER BY id;",
            "select * from t where value LIKE '\\%AAA%' {escape '\\'} ORDER BY id;"
        );

        check(
            "select * from t where value LIKE '\\%AAA''s%' ESCAPE '\\'",
            "select * from t where value LIKE '\\%AAA''s%' {escape '\\'}"
        );
    }

    /**
     * Test escape sequences with additional whitespace characters
     */
    public void testLikeEscapeSequenceWithWhitespaces() throws Exception {
        check("ESCAPE '\\'", "{ '\\' }");
        check("ESCAPE '\\'", "{ escape '\\'}");

        check("ESCAPE '\\'", "{   '\\' }");
        check("ESCAPE '\\'", "{   escape   '\\' }");

        check("ESCAPE '\\'", "{ \n '\\' }");
        check("ESCAPE '\\'", "{ \n escape\n'\\' }");
    }

    /**
     * Test invalid escape sequence.
     */
    public void testLikeOnInvalidLikeEscapeSequence() {
        checkFail("LIKE 'AAA's'");
        checkFail("LIKE 'AAA\'s'");

        checkFail("LIKE '\\%AAA%' {escape'\\' }");

        checkFail("LIKE '\\%AAA%' {'\\' ORDER BY id");
        checkFail("LIKE '\\%AAA%' {escape '\\' ORDER BY id;");

        checkFail("LIKE '\\%AAA%' '\\'} ORDER BY id");
        checkFail("LIKE '\\%AAA%' escape '\\'} ORDER BY id;");
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
