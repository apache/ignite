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

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Period;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteStdSqlOperatorTable;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;

/**
 * Ignite's SQL dialect test.
 * This test contains basic checks for standard SQL operators (only syntax check and check ability to use it).
 *
 * @see IgniteStdSqlOperatorTable
 */
public class StdSqlOperatorsTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        sql("CREATE TABLE t(val INT)");
        sql("INSERT INTO t VALUES (1)");
    }

    /** */
    @Test
    public void testSet() {
        assertQuery("SELECT 1 UNION SELECT 2").returns(1).returns(2).check();
        assertQuery("SELECT 1 UNION ALL SELECT 1").returns(1).returns(1).check();
        assertQuery("SELECT 1 EXCEPT SELECT 2").returns(1).check();
        assertQuery("SELECT 1 EXCEPT ALL SELECT 2").returns(1).check();
        assertQuery("SELECT 1 INTERSECT SELECT 1").returns(1).check();
        assertQuery("SELECT 1 INTERSECT ALL SELECT 1").returns(1).check();
    }

    /** */
    @Test
    public void testLogical() {
        assertExpression("FALSE AND TRUE").returns(false).check();
        assertExpression("FALSE OR TRUE").returns(true).check();
        assertExpression("NOT FALSE").returns(true).check();
    }

    /** */
    @Test
    public void testComparison() {
        assertExpression("2 > 1").returns(true).check();
        assertExpression("2 >= 1").returns(true).check();
        assertExpression("2 < 1").returns(false).check();
        assertExpression("2 <= 1").returns(false).check();
        assertExpression("2 = 1").returns(false).check();
        assertExpression("2 <> 1").returns(true).check();
        assertExpression("2 BETWEEN 1 AND 3").returns(true).check();
        assertExpression("2 NOT BETWEEN 1 AND 3").returns(false).check();
    }

    /** */
    @Test
    public void testArithmetic() {
        assertExpression("1 + 2").returns(3).check();
        assertExpression("2 - 1").returns(1).check();
        assertExpression("2 * 3").returns(6).check();
        assertExpression("3 / 2 ").returns(1).check();
        assertExpression("-(1)").returns(-1).check();
        assertExpression("+(1)").returns(1).check();
        assertExpression("3 % 2").returns(1).check();
    }

    /** */
    @Test
    public void testAggregates() {
        assertExpression("COUNT(*)").returns(1L).check();
        assertExpression("SUM(val)").returns(1L).check();
        assertExpression("AVG(val)").returns(1).check();
        assertExpression("MIN(val)").returns(1).check();
        assertExpression("MAX(val)").returns(1).check();
        assertExpression("ANY_VALUE(val)").returns(1).check();
        assertExpression("COUNT(*) FILTER(WHERE val <> 1)").returns(0L).check();
        assertExpression("LISTAGG(val, ',') WITHIN GROUP (ORDER BY val DESC)").returns("1").check();
        assertExpression("GROUP_CONCAT(val, ',' ORDER BY val DESC)").returns("1").check();
        assertExpression("STRING_AGG(val, ',' ORDER BY val DESC)").returns("1").check();
        assertExpression("ARRAY_AGG(val ORDER BY val DESC)").returns(Collections.singletonList(1)).check();
        assertQuery("SELECT ARRAY_CONCAT_AGG(a ORDER BY CARDINALITY(a)) FROM " +
            "(SELECT 1 as id, ARRAY[3, 4, 5, 6] as a UNION SELECT 1 as id, ARRAY[1, 2] as a) GROUP BY id")
            .returns((IntStream.range(1, 7).boxed().collect(Collectors.toList()))).check();
        assertExpression("EVERY(val = 1)").returns(true).check();
        assertExpression("SOME(val = 1)").returns(true).check();
    }

    /** */
    @Test
    public void testIs() {
        assertExpression("'a' IS NULL").returns(false).check();
        assertExpression("'a' IS NOT NULL").returns(true).check();
        assertExpression("1=1 IS TRUE").returns(true).check();
        assertExpression("1=1 IS NOT TRUE").returns(false).check();
        assertExpression("1=1 IS FALSE").returns(false).check();
        assertExpression("1=1 IS NOT FALSE").returns(true).check();
        assertExpression("NULL IS DISTINCT FROM NULL").returns(false).check();
        assertExpression("NULL IS NOT DISTINCT FROM NULL").returns(true).check();
    }

    /** */
    @Test
    public void testLike() {
        assertExpression("'a' LIKE 'a%'").returns(true).check();
        assertExpression("'a' NOT LIKE 'a%'").returns(false).check();
        assertExpression("'a' SIMILAR TO '(a|A)%'").returns(true).check();
        assertExpression("'A' NOT SIMILAR TO '(a|A)%'").returns(false).check();
    }

    /** */
    @Test
    public void testNullsOrdering() {
        assertQuery("SELECT a FROM (SELECT 1 AS a) ORDER BY a NULLS FIRST").returns(1).check();
        assertQuery("SELECT a FROM (SELECT 1 AS a) ORDER BY a NULLS LAST").returns(1).check();
        assertQuery("SELECT a FROM (SELECT 1 AS a) ORDER BY a DESC NULLS FIRST").returns(1).check();
        assertQuery("SELECT a FROM (SELECT 1 AS a) ORDER BY a DESC NULLS LAST").returns(1).check();
        assertQuery("SELECT a FROM (SELECT 1 AS a) ORDER BY a ASC NULLS FIRST").returns(1).check();
        assertQuery("SELECT a FROM (SELECT 1 AS a) ORDER BY a ASC NULLS LAST").returns(1).check();
    }

    /** */
    @Test
    public void testExists() {
        assertExpression("EXISTS (SELECT 1)").returns(true).check();
        assertExpression("NOT EXISTS (SELECT 1)").returns(false).check();
    }

    /** */
    @Test
    public void testStringFunctions() {
        assertExpression("UPPER('aA')").returns("AA").check();
        assertExpression("LOWER('aA')").returns("aa").check();
        assertExpression("INITCAP('aA')").returns("Aa").check();
        assertExpression("TO_BASE64('aA')").returns("YUE=").check();
        assertExpression("FROM_BASE64('YUE=')::VARCHAR").returns("aA").check();
        assertExpression("MD5('aa')").returns("4124bc0a9335c27f086f24ba207a4912").check();
        assertExpression("SHA1('aa')").returns("e0c9035898dd52fc65c41454cec9c4d2611bfb37").check();
        assertExpression("SUBSTRING('aAaA', 2, 2)").returns("Aa").check();
        assertExpression("LEFT('aA', 1)").returns("a").check();
        assertExpression("RIGHT('aA', 1)").returns("A").check();
        assertExpression("REPLACE('aA', 'A', 'a')").returns("aa").check();
        assertExpression("TRANSLATE('aA', 'A', 'a')").returns("aa").check();
        assertExpression("CHR(97)").returns("a").check();
        assertExpression("CHAR_LENGTH('aa')").returns(2).check();
        assertExpression("CHARACTER_LENGTH('aa')").returns(2).check();
        assertExpression("'a' || 'a'").returns("aa").check();
        assertExpression("CONCAT('a', 'a')").returns("aa").check();
        assertExpression("OVERLAY('aAaA' PLACING 'aA' FROM 2)").returns("aaAA").check();
        assertExpression("POSITION('A' IN 'aA')").returns(2).check();
        assertExpression("ASCII('a')").returns(97).check();
        assertExpression("REPEAT('a', 2)").returns("aa").check();
        assertExpression("SPACE(2)").returns("  ").check();
        assertExpression("STRCMP('a', 'b')").returns(1).check();
        assertExpression("SOUNDEX('a')").returns("A000").check();
        assertExpression("DIFFERENCE('a', 'A')").returns(4).check();
        assertExpression("REVERSE('aA')").returns("Aa").check();
        assertExpression("TRIM('a' FROM 'aA')").returns("A").check();
        assertExpression("LTRIM(' a ')").returns("a ").check();
        assertExpression("RTRIM(' a ')").returns(" a").check();
    }

    /** */
    @Test
    public void testMathFunctions() {
        assertExpression("MOD(3, 2)").returns(1).check();
        assertExpression("EXP(2)").returns(Math.exp(2)).check();
        assertExpression("POWER(2, 2)").returns(Math.pow(2, 2)).check();
        assertExpression("LN(2)").returns(Math.log(2)).check();
        assertExpression("LOG10(2)").returns(Math.log(2) / Math.log(10)).check();
        assertExpression("ABS(-1)").returns(Math.abs(-1)).check();
        assertExpression("RAND()").check();
        assertExpression("RAND_INTEGER(10)").check();
        assertExpression("ACOS(1)").returns(Math.acos(1)).check();
        assertExpression("ACOSH(1)").returns(0d).check();
        assertExpression("ASIN(1)").returns(Math.asin(1)).check();
        assertExpression("ASINH(0)").returns(0d).check();
        assertExpression("ATAN(1)").returns(Math.atan(1)).check();
        assertExpression("ATANH(0)").returns(0d).check();
        assertExpression("ATAN2(1, 1)").returns(Math.atan2(1, 1)).check();
        assertExpression("SQRT(4)").returns(Math.sqrt(4)).check();
        assertExpression("CBRT(8)").returns(Math.cbrt(8)).check();
        assertExpression("COS(1)").returns(Math.cos(1)).check();
        assertExpression("COSH(1)").returns(Math.cosh(1)).check();
        assertExpression("COT(1)").returns(1.0d / Math.tan(1)).check();
        assertExpression("COTH(1)").returns(1.0d / Math.tanh(1)).check();
        assertExpression("DEGREES(1)").returns(Math.toDegrees(1)).check();
        assertExpression("RADIANS(1)").returns(Math.toRadians(1)).check();
        assertExpression("ROUND(1.7)").returns(BigDecimal.valueOf(2)).check();
        assertExpression("SIGN(-5)").returns(-1).check();
        assertExpression("SIN(1)").returns(Math.sin(1)).check();
        assertExpression("SINH(1)").returns(Math.sinh(1)).check();
        assertExpression("TAN(1)").returns(Math.tan(1)).check();
        assertExpression("TANH(1)").returns(Math.tanh(1)).check();
        assertExpression("SEC(1)").returns(1d / Math.cos(1)).check();
        assertExpression("SECH(1)").returns(1d / Math.cosh(1)).check();
        assertExpression("CSC(1)").returns(1d / Math.sin(1)).check();
        assertExpression("CSCH(1)").returns(1d / Math.sinh(1)).check();
        assertExpression("TRUNCATE(1.7)").returns(BigDecimal.valueOf(1)).check();
        assertExpression("PI").returns(Math.PI).check();
    }

    /** */
    @Test
    public void testDateAndTime() {
        assertExpression("DATE '2021-01-01' + interval (1) days").returns(Date.valueOf("2021-01-02")).check();
        assertExpression("(DATE '2021-03-01' - DATE '2021-01-01') months").returns(Period.ofMonths(2)).check();
        assertExpression("EXTRACT(DAY FROM DATE '2021-01-15')").returns(15L).check();
        assertExpression("FLOOR(DATE '2021-01-15' TO MONTH)").returns(Date.valueOf("2021-01-01")).check();
        assertExpression("CEIL(DATE '2021-01-15' TO MONTH)").returns(Date.valueOf("2021-02-01")).check();
        assertExpression("TIMESTAMPADD(DAY, 1, TIMESTAMP '2021-01-01')").returns(Timestamp.valueOf("2021-01-02 00:00:00")).check();
        assertExpression("TIMESTAMPDIFF(DAY, TIMESTAMP '2021-01-01', TIMESTAMP '2021-01-02')").returns(1).check();
        assertExpression("LAST_DAY(DATE '2021-01-01')").returns(Date.valueOf("2021-01-31")).check();
        assertExpression("DAYNAME(DATE '2021-01-01')").returns("Friday").check();
        assertExpression("MONTHNAME(DATE '2021-01-01')").returns("January").check();
        assertExpression("DAYOFMONTH(DATE '2021-01-01')").returns(1L).check();
        assertExpression("DAYOFWEEK(DATE '2021-01-01')").returns(6L).check();
        assertExpression("DAYOFYEAR(DATE '2021-01-01')").returns(1L).check();
        assertExpression("YEAR(DATE '2021-01-01')").returns(2021L).check();
        assertExpression("QUARTER(DATE '2021-01-01')").returns(1L).check();
        assertExpression("MONTH(DATE '2021-01-01')").returns(1L).check();
        assertExpression("WEEK(DATE '2021-01-04')").returns(1L).check();
        assertExpression("HOUR(TIMESTAMP '2021-01-01 01:01:01')").returns(1L).check();
        assertExpression("MINUTE(TIMESTAMP '2021-01-01 01:01:01')").returns(1L).check();
        assertExpression("SECOND(TIMESTAMP '2021-01-01 01:01:01')").returns(1L).check();
        assertExpression("TIMESTAMP_SECONDS(1609459200)").returns(Timestamp.valueOf("2021-01-01 00:00:00")).check();
        assertExpression("TIMESTAMP_MILLIS(1609459200000)").returns(Timestamp.valueOf("2021-01-01 00:00:00")).check();
        assertExpression("TIMESTAMP_MICROS(1609459200000000)").returns(Timestamp.valueOf("2021-01-01 00:00:00")).check();
        assertExpression("UNIX_SECONDS(TIMESTAMP '2021-01-01 00:00:00')").returns(1609459200L).check();
        assertExpression("UNIX_MILLIS(TIMESTAMP '2021-01-01 00:00:00')").returns(1609459200000L).check();
        assertExpression("UNIX_MICROS(TIMESTAMP '2021-01-01 00:00:00')").returns(1609459200000000L).check();
        assertExpression("UNIX_DATE(DATE '2021-01-01')").returns(18628).check();
        assertExpression("DATE_FROM_UNIX_DATE(18628)").returns(Date.valueOf("2021-01-01")).check();
        assertExpression("DATE('2021-01-01')").returns(Date.valueOf("2021-01-01")).check();
        assertExpression("TIME(1, 10, 30)").returns(Time.valueOf("01:10:30")).check();
        assertExpression("DATETIME(2021, 1, 1, 1, 10, 30)").returns(Timestamp.valueOf("2021-01-01 01:10:30")).check();
        assertExpression("TO_CHAR(DATE '2021-01-01', 'YYMMDD')").returns("210101").check();
        assertExpression("TO_DATE('210101', 'YYMMDD')").returns(Date.valueOf("2021-01-01")).check();
        assertExpression("TO_TIMESTAMP('210101-01-10-30', 'YYMMDD-HH24-MI-SS')").returns(Timestamp.valueOf("2021-01-01 01:10:30")).check();
    }

    /** */
    @Test
    public void testPosixRegex() {
        assertExpression("'aA' ~ '.*aa.*'").returns(false).check();
        assertExpression("'aA' ~* '.*aa.*'").returns(true).check();
        assertExpression("'aA' !~ '.*aa.*'").returns(true).check();
        assertExpression("'aA' !~* '.*aa.*'").returns(false).check();
        assertExpression("REGEXP_REPLACE('aA', '[Aa]+', 'X')").returns("X").check();
    }

    /** */
    @Test
    public void testCollections() {
        assertExpression("MAP(SELECT 'a', 1)").returns(F.asMap("a", 1)).check();
        assertExpression("ARRAY(SELECT 1)").returns(Collections.singletonList(1)).check();
        assertExpression("MAP['a', 1, 'A', 2]").returns(F.asMap("a", 1, "A", 2)).check();
        assertExpression("ARRAY[1, 2, 3]").returns(Arrays.asList(1, 2, 3)).check();
        assertExpression("ARRAY[1, 2, 3][2]").returns(2).check();
        assertExpression("CARDINALITY(ARRAY[1, 2, 3])").returns(3).check();
        assertExpression("ARRAY[1, 2, 3] IS EMPTY").returns(false).check();
        assertExpression("ARRAY[1, 2, 3] IS NOT EMPTY").returns(true).check();
        assertQuery("SELECT * FROM UNNEST(ARRAY[1, 2]) WITH ORDINALITY").returns(1, 1).returns(2, 2)
            .check();
    }

    /** */
    @Test
    public void testOtherFunctions() {
        assertQuery("SELECT * FROM (VALUES ROW('a', 1))").returns("a", 1).check();
        assertExpression("CAST('1' AS INT)").returns(1).check();
        assertExpression("'1'::INT").returns(1).check();
        assertExpression("COALESCE(null, 'a', 'A')").returns("a").check();
        assertExpression("NVL(null, 'a')").returns("a").check();
        assertExpression("NULLIF(1, 2)").returns(1).check();
        assertExpression("CASE WHEN 1=1 THEN 1 ELSE 2 END").returns(1).check();
        assertExpression("DECODE(1, 1, 1, 2)").returns(1).check();
        assertExpression("LEAST('a', 'b')").returns("a").check();
        assertExpression("GREATEST('a', 'b')").returns("b").check();
        assertExpression("COMPRESS('')::VARCHAR").returns("").check();
        assertExpression("OCTET_LENGTH(x'01')").returns(1).check();
        assertExpression("CAST(INTERVAL 1 SECONDS AS INT)").returns(1).check(); // Converted to REINTERPRED.
    }

    /** */
    @Test
    public void testXml() {
        assertExpression("EXTRACTVALUE('<a>b</a>', '//a')").returns("b").check();
        assertExpression("XMLTRANSFORM('<a>b</a>',"
            + "'<?xml version=\"1.0\"?>\n"
            + "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">"
            + "  <xsl:output method=\"text\"/>"
            + "  <xsl:template match=\"/\">"
            + "    a - <xsl:value-of select=\"/a\"/>"
            + "  </xsl:template>"
            + "</xsl:stylesheet>')"
        ).returns("    a - b").check();
        assertExpression("\"EXTRACT\"('<a><b>c</b></a>', '/a/b')").returns("<b>c</b>").check();
        assertExpression("EXISTSNODE('<a><b>c</b></a>', '/a/b')").returns(1).check();
    }

    /** */
    @Test
    public void testJson() {
        assertExpression("'{\"a\":1}' FORMAT JSON").check();
        assertExpression("JSON_VALUE('{\"a\":1}', '$.a')").returns("1").check();
        assertExpression("JSON_VALUE('{\"a\":1}' FORMAT JSON, '$.a')").returns("1").check();
        assertExpression("JSON_QUERY('{\"a\":{\"b\":1}}', '$.a')").returns("{\"b\":1}").check();
        assertExpression("JSON_TYPE('{\"a\":1}')").returns("OBJECT").check();
        assertExpression("JSON_EXISTS('{\"a\":1}', '$.a')").returns(true).check();
        assertExpression("JSON_DEPTH('{\"a\":1}')").returns(2).check();
        assertExpression("JSON_KEYS('{\"a\":1}')").returns("[\"a\"]").check();
        assertExpression("JSON_PRETTY('{\"a\":1}')").returns("{\n  \"a\" : 1\n}").check();
        assertExpression("JSON_LENGTH('{\"a\":1}')").returns(1).check();
        assertExpression("JSON_REMOVE('{\"a\":1, \"b\":2}', '$.a')").returns("{\"b\":2}").check();
        assertExpression("JSON_STORAGE_SIZE('1')").returns(1).check();
        assertExpression("JSON_OBJECT('a': 1)").returns("{\"a\":1}").check();
        assertExpression("JSON_ARRAY('a', 'b')").returns("[\"a\",\"b\"]").check();
        assertExpression("'{\"a\":1}' IS JSON").returns(true).check();
        assertExpression("'{\"a\":1}' IS JSON VALUE").returns(true).check();
        assertExpression("'{\"a\":1}' IS JSON OBJECT").returns(true).check();
        assertExpression("'[1, 2]' IS JSON ARRAY").returns(true).check();
        assertExpression("'1' IS JSON SCALAR").returns(true).check();
        assertExpression("'{\"a\":1}' IS NOT JSON").returns(false).check();
        assertExpression("'{\"a\":1}' IS NOT JSON VALUE").returns(false).check();
        assertExpression("'{\"a\":1}' IS NOT JSON OBJECT").returns(false).check();
        assertExpression("'[1, 2]' IS NOT JSON ARRAY").returns(false).check();
        assertExpression("'1' IS NOT JSON SCALAR").returns(false).check();
    }

    /** */
    @Test
    public void testCurrentTimeFunctions() {
        // Don't check returned value, only ability to use these functions.
        assertExpression("CURRENT_TIME").check();
        assertExpression("CURRENT_TIMESTAMP").check();
        assertExpression("CURRENT_DATE").check();
        assertExpression("LOCALTIME").check();
        assertExpression("LOCALTIMESTAMP").check();
    }

    /** */
    private QueryChecker assertExpression(String qry) {
        // Select expressions from table to test plan serialization containing these expressions.
        return assertQuery("SELECT " + qry + " FROM t");
    }
}
