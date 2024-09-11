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

package org.apache.ignite.internal.processors.query.calcite;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Period;
import java.util.Arrays;
import java.util.Collections;
import org.apache.ignite.internal.processors.query.calcite.integration.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteStdSqlOperatorTable;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Ignite's SQL dialect test.
 * This test contains basic checks for standard SQL operators (only syntax check and check ability to use it).
 *
 * @see IgniteStdSqlOperatorTable
 */
public class StdSqlOperatorsTest extends AbstractBasicIntegrationTest {
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
        assertQuery("SELECT FALSE AND TRUE").returns(false).check();
        assertQuery("SELECT FALSE OR TRUE").returns(true).check();
        assertQuery("SELECT NOT FALSE").returns(true).check();
    }

    /** */
    @Test
    public void testComparison() {
        assertQuery("SELECT 2 > 1").returns(true).check();
        assertQuery("SELECT 2 >= 1").returns(true).check();
        assertQuery("SELECT 2 < 1").returns(false).check();
        assertQuery("SELECT 2 <= 1").returns(false).check();
        assertQuery("SELECT 2 = 1").returns(false).check();
        assertQuery("SELECT 2 <> 1").returns(true).check();
        assertQuery("SELECT 2 BETWEEN 1 AND 3").returns(true).check();
        assertQuery("SELECT 2 NOT BETWEEN 1 AND 3").returns(false).check();
    }

    /** */
    @Test
    public void testArithmetic() {
        assertQuery("SELECT 1 + 2").returns(3).check();
        assertQuery("SELECT 2 - 1").returns(1).check();
        assertQuery("SELECT 2 * 3").returns(6).check();
        assertQuery("SELECT 3 / 2 ").returns(1).check();
        assertQuery("SELECT -(1)").returns(-1).check();
        assertQuery("SELECT +(1)").returns(1).check();
        assertQuery("SELECT 3 % 2").returns(1).check();
    }

    /** */
    @Test
    public void testAggregates() {
        assertQuery("SELECT COUNT(*) FROM (SELECT 1 AS a)").returns(1L).check();
        assertQuery("SELECT SUM(a) FROM (SELECT 1 AS a)").returns(1L).check();
        assertQuery("SELECT AVG(a) FROM (SELECT 1 AS a)").returns(1).check();
        assertQuery("SELECT MIN(a) FROM (SELECT 1 AS a)").returns(1).check();
        assertQuery("SELECT MAX(a) FROM (SELECT 1 AS a)").returns(1).check();
        assertQuery("SELECT ANY_VALUE(a) FROM (SELECT 1 AS a)").returns(1).check();
        assertQuery("SELECT COUNT(*) FILTER(WHERE a <> 1) FROM (SELECT 1 AS a)").returns(0L).check();
    }

    /** */
    @Test
    public void testIs() {
        assertQuery("SELECT 'a' IS NULL").returns(false).check();
        assertQuery("SELECT 'a' IS NOT NULL").returns(true).check();
        assertQuery("SELECT 1=1 IS TRUE").returns(true).check();
        assertQuery("SELECT 1=1 IS NOT TRUE").returns(false).check();
        assertQuery("SELECT 1=1 IS FALSE").returns(false).check();
        assertQuery("SELECT 1=1 IS NOT FALSE").returns(true).check();
    }

    /** */
    @Test
    public void testLike() {
        assertQuery("SELECT 'a' LIKE 'a%'").returns(true).check();
        assertQuery("SELECT 'a' NOT LIKE 'a%'").returns(false).check();
        assertQuery("SELECT 'a' SIMILAR TO '(a|A)%'").returns(true).check();
        assertQuery("SELECT 'A' NOT SIMILAR TO '(a|A)%'").returns(false).check();
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
        assertQuery("SELECT EXISTS (SELECT 1)").returns(true).check();
        assertQuery("SELECT NOT EXISTS (SELECT 1)").returns(false).check();
    }

    /** */
    @Test
    public void testStringFunctions() {
        assertQuery("SELECT UPPER('aA')").returns("AA").check();
        assertQuery("SELECT LOWER('aA')").returns("aa").check();
        assertQuery("SELECT INITCAP('aA')").returns("Aa").check();
        assertQuery("SELECT TO_BASE64('aA')").returns("YUE=").check();
        assertQuery("SELECT FROM_BASE64('YUE=')::VARCHAR").returns("aA").check();
        assertQuery("SELECT MD5('aa')").returns("4124bc0a9335c27f086f24ba207a4912").check();
        assertQuery("SELECT SHA1('aa')").returns("e0c9035898dd52fc65c41454cec9c4d2611bfb37").check();
        assertQuery("SELECT SUBSTRING('aAaA', 2, 2)").returns("Aa").check();
        assertQuery("SELECT LEFT('aA', 1)").returns("a").check();
        assertQuery("SELECT RIGHT('aA', 1)").returns("A").check();
        assertQuery("SELECT REPLACE('aA', 'A', 'a')").returns("aa").check();
        assertQuery("SELECT TRANSLATE('aA', 'A', 'a')").returns("aa").check();
        assertQuery("SELECT CHR(97)").returns("a").check();
        assertQuery("SELECT CHAR_LENGTH('aa')").returns(2).check();
        assertQuery("SELECT CHARACTER_LENGTH('aa')").returns(2).check();
        assertQuery("SELECT 'a' || 'a'").returns("aa").check();
        assertQuery("SELECT CONCAT('a', 'a')").returns("aa").check();
        assertQuery("SELECT OVERLAY('aAaA' PLACING 'aA' FROM 2)").returns("aaAA").check();
        assertQuery("SELECT POSITION('A' IN 'aA')").returns(2).check();
        assertQuery("SELECT ASCII('a')").returns(97).check();
        assertQuery("SELECT REPEAT('a', 2)").returns("aa").check();
        assertQuery("SELECT SPACE(2)").returns("  ").check();
        assertQuery("SELECT STRCMP('a', 'b')").returns(1).check();
        assertQuery("SELECT SOUNDEX('a')").returns("A000").check();
        assertQuery("SELECT DIFFERENCE('a', 'A')").returns(4).check();
        assertQuery("SELECT REVERSE('aA')").returns("Aa").check();
        assertQuery("SELECT TRIM('a' FROM 'aA')").returns("A").check();
        assertQuery("SELECT LTRIM(' a ')").returns("a ").check();
        assertQuery("SELECT RTRIM(' a ')").returns(" a").check();
    }

    /** */
    @Test
    public void testMathFunctions() {
        assertQuery("SELECT MOD(3, 2)").returns(1).check();
        assertQuery("SELECT EXP(2)").returns(Math.exp(2)).check();
        assertQuery("SELECT POWER(2, 2)").returns(Math.pow(2, 2)).check();
        assertQuery("SELECT LN(2)").returns(Math.log(2)).check();
        assertQuery("SELECT LOG10(2) ").returns(Math.log10(2)).check();
        assertQuery("SELECT ABS(-1)").returns(Math.abs(-1)).check();
        assertQuery("SELECT RAND()").check();
        assertQuery("SELECT RAND_INTEGER(10)").check();
        assertQuery("SELECT ACOS(1)").returns(Math.acos(1)).check();
        assertQuery("SELECT ASIN(1)").returns(Math.asin(1)).check();
        assertQuery("SELECT ATAN(1)").returns(Math.atan(1)).check();
        assertQuery("SELECT ATAN2(1, 1)").returns(Math.atan2(1, 1)).check();
        assertQuery("SELECT SQRT(4)").returns(Math.sqrt(4)).check();
        assertQuery("SELECT CBRT(8)").returns(Math.cbrt(8)).check();
        assertQuery("SELECT COS(1)").returns(Math.cos(1)).check();
        assertQuery("SELECT COSH(1)").returns(Math.cosh(1)).check();
        assertQuery("SELECT COT(1)").returns(1.0d / Math.tan(1)).check();
        assertQuery("SELECT DEGREES(1)").returns(Math.toDegrees(1)).check();
        assertQuery("SELECT RADIANS(1)").returns(Math.toRadians(1)).check();
        assertQuery("SELECT ROUND(1.7)").returns(BigDecimal.valueOf(2)).check();
        assertQuery("SELECT SIGN(-5)").returns(-1).check();
        assertQuery("SELECT SIN(1)").returns(Math.sin(1)).check();
        assertQuery("SELECT SINH(1)").returns(Math.sinh(1)).check();
        assertQuery("SELECT TAN(1)").returns(Math.tan(1)).check();
        assertQuery("SELECT TANH(1)").returns(Math.tanh(1)).check();
        assertQuery("SELECT TRUNCATE(1.7)").returns(BigDecimal.valueOf(1)).check();
        assertQuery("SELECT PI").returns(Math.PI).check();
    }

    /** */
    @Test
    public void testDateAndTime() {
        assertQuery("SELECT DATE '2021-01-01' + interval (1) days").returns(Date.valueOf("2021-01-02")).check();
        assertQuery("SELECT (DATE '2021-03-01' - DATE '2021-01-01') months").returns(Period.ofMonths(2)).check();
        assertQuery("SELECT EXTRACT(DAY FROM DATE '2021-01-15')").returns(15L).check();
        assertQuery("SELECT FLOOR(DATE '2021-01-15' TO MONTH)").returns(Date.valueOf("2021-01-01")).check();
        assertQuery("SELECT CEIL(DATE '2021-01-15' TO MONTH)").returns(Date.valueOf("2021-02-01")).check();
        assertQuery("SELECT TIMESTAMPADD(DAY, 1, TIMESTAMP '2021-01-01')").returns(Timestamp.valueOf("2021-01-02 00:00:00")).check();
        assertQuery("SELECT TIMESTAMPDIFF(DAY, TIMESTAMP '2021-01-01', TIMESTAMP '2021-01-02')").returns(1).check();
        assertQuery("SELECT LAST_DAY(DATE '2021-01-01')").returns(Date.valueOf("2021-01-31")).check();
        assertQuery("SELECT DAYNAME(DATE '2021-01-01')").returns("Friday").check();
        assertQuery("SELECT MONTHNAME(DATE '2021-01-01')").returns("January").check();
        assertQuery("SELECT DAYOFMONTH(DATE '2021-01-01')").returns(1L).check();
        assertQuery("SELECT DAYOFWEEK(DATE '2021-01-01')").returns(6L).check();
        assertQuery("SELECT DAYOFYEAR(DATE '2021-01-01')").returns(1L).check();
        assertQuery("SELECT YEAR(DATE '2021-01-01')").returns(2021L).check();
        assertQuery("SELECT QUARTER(DATE '2021-01-01')").returns(1L).check();
        assertQuery("SELECT MONTH(DATE '2021-01-01')").returns(1L).check();
        assertQuery("SELECT WEEK(DATE '2021-01-04')").returns(1L).check();
        assertQuery("SELECT HOUR(TIMESTAMP '2021-01-01 01:01:01')").returns(1L).check();
        assertQuery("SELECT MINUTE(TIMESTAMP '2021-01-01 01:01:01')").returns(1L).check();
        assertQuery("SELECT SECOND(TIMESTAMP '2021-01-01 01:01:01')").returns(1L).check();
        assertQuery("SELECT TIMESTAMP_SECONDS(1609459200)").returns(Timestamp.valueOf("2021-01-01 00:00:00")).check();
        assertQuery("SELECT TIMESTAMP_MILLIS(1609459200000)").returns(Timestamp.valueOf("2021-01-01 00:00:00")).check();
        assertQuery("SELECT TIMESTAMP_MICROS(1609459200000000)").returns(Timestamp.valueOf("2021-01-01 00:00:00")).check();
        assertQuery("SELECT UNIX_SECONDS(TIMESTAMP '2021-01-01 00:00:00')").returns(1609459200L).check();
        assertQuery("SELECT UNIX_MILLIS(TIMESTAMP '2021-01-01 00:00:00')").returns(1609459200000L).check();
        assertQuery("SELECT UNIX_MICROS(TIMESTAMP '2021-01-01 00:00:00')").returns(1609459200000000L).check();
        assertQuery("SELECT UNIX_DATE(DATE '2021-01-01')").returns(18628).check();
        assertQuery("SELECT DATE_FROM_UNIX_DATE(18628)").returns(Date.valueOf("2021-01-01")).check();
        assertQuery("SELECT DATE('2021-01-01')").returns(Date.valueOf("2021-01-01")).check();
    }

    /** */
    @Test
    public void testPosixRegex() {
        assertQuery("SELECT 'aA' ~ '.*aa.*'").returns(false).check();
        assertQuery("SELECT 'aA' ~* '.*aa.*'").returns(true).check();
        assertQuery("SELECT 'aA' !~ '.*aa.*'").returns(true).check();
        assertQuery("SELECT 'aA' !~* '.*aa.*'").returns(false).check();
        assertQuery("SELECT REGEXP_REPLACE('aA', '[Aa]+', 'X')").returns("X").check();
    }

    /** */
    @Test
    public void testCollections() {
        assertQuery("SELECT MAP['a', 1, 'A', 2]").returns(F.asMap("a", 1, "A", 2)).check();
        assertQuery("SELECT ARRAY[1, 2, 3]").returns(Arrays.asList(1, 2, 3)).check();
        assertQuery("SELECT ARRAY[1, 2, 3][2]").returns(2).check();
        assertQuery("SELECT CARDINALITY(ARRAY[1, 2, 3])").returns(3).check();
        assertQuery("SELECT ARRAY[1, 2, 3] IS EMPTY").returns(false).check();
        assertQuery("SELECT ARRAY[1, 2, 3] IS NOT EMPTY").returns(true).check();
    }

    /** */
    @Test
    @Ignore("https://issues.apache.org/jira/browse/IGNITE-15550")
    public void testQueryAsCollections() {
        assertQuery("SELECT MAP(SELECT 'a', 1)").returns(F.asMap("a", 1)).check();
        assertQuery("SELECT ARRAY(SELECT 1)").returns(Collections.singletonList(1)).check();
    }

    /** */
    @Test
    public void testOtherFunctions() {
        assertQuery("SELECT * FROM (VALUES ROW('a', 1))").returns("a", 1).check();
        assertQuery("SELECT CAST('1' AS INT)").returns(1).check();
        assertQuery("SELECT '1'::INT").returns(1).check();
        assertQuery("SELECT COALESCE(null, 'a', 'A')").returns("a").check();
        assertQuery("SELECT NVL(null, 'a')").returns("a").check();
        assertQuery("SELECT NULLIF(1, 2)").returns(1).check();
        assertQuery("SELECT CASE WHEN 1=1 THEN 1 ELSE 2 END").returns(1).check();
        assertQuery("SELECT DECODE(1, 1, 1, 2)").returns(1).check();
        assertQuery("SELECT LEAST('a', 'b')").returns("a").check();
        assertQuery("SELECT GREATEST('a', 'b')").returns("b").check();
        assertQuery("SELECT COMPRESS('')::VARCHAR").returns("").check();
        assertQuery("SELECT OCTET_LENGTH(x'01')").returns(1).check();
    }

    /** */
    @Test
    public void testXml() {
        assertQuery("SELECT EXTRACTVALUE('<a>b</a>', '//a')").returns("b").check();
        assertQuery("SELECT XMLTRANSFORM('<a>b</a>',"
            + "'<?xml version=\"1.0\"?>\n"
            + "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">"
            + "  <xsl:output method=\"text\"/>"
            + "  <xsl:template match=\"/\">"
            + "    a - <xsl:value-of select=\"/a\"/>"
            + "  </xsl:template>"
            + "</xsl:stylesheet>')"
        ).returns("    a - b").check();
        assertQuery("SELECT \"EXTRACT\"('<a><b>c</b></a>', '/a/b')").returns("<b>c</b>").check();
        assertQuery("SELECT EXISTSNODE('<a><b>c</b></a>', '/a/b')").returns(1).check();
    }

    /** */
    @Test
    public void testJson() {
        assertQuery("SELECT '{\"a\":1}' FORMAT JSON").check();
        assertQuery("SELECT JSON_VALUE('{\"a\":1}', '$.a')").returns("1").check();
        assertQuery("SELECT JSON_VALUE('{\"a\":1}' FORMAT JSON, '$.a')").returns("1").check();
        assertQuery("SELECT JSON_QUERY('{\"a\":{\"b\":1}}', '$.a')").returns("{\"b\":1}").check();
        assertQuery("SELECT JSON_TYPE('{\"a\":1}')").returns("OBJECT").check();
        assertQuery("SELECT JSON_EXISTS('{\"a\":1}', '$.a')").returns(true).check();
        assertQuery("SELECT JSON_DEPTH('{\"a\":1}')").returns(2).check();
        assertQuery("SELECT JSON_KEYS('{\"a\":1}')").returns("[\"a\"]").check();
        assertQuery("SELECT JSON_PRETTY('{\"a\":1}')").returns("{\n  \"a\" : 1\n}").check();
        assertQuery("SELECT JSON_LENGTH('{\"a\":1}')").returns(1).check();
        assertQuery("SELECT JSON_REMOVE('{\"a\":1, \"b\":2}', '$.a')").returns("{\"b\":2}").check();
        assertQuery("SELECT JSON_STORAGE_SIZE('1')").returns(1).check();
        assertQuery("SELECT JSON_OBJECT('a': 1)").returns("{\"a\":1}").check();
        assertQuery("SELECT JSON_ARRAY('a', 'b')").returns("[\"a\",\"b\"]").check();
        assertQuery("SELECT '{\"a\":1}' IS JSON").returns(true).check();
        assertQuery("SELECT '{\"a\":1}' IS JSON VALUE").returns(true).check();
        assertQuery("SELECT '{\"a\":1}' IS JSON OBJECT").returns(true).check();
        assertQuery("SELECT '[1, 2]' IS JSON ARRAY").returns(true).check();
        assertQuery("SELECT '1' IS JSON SCALAR").returns(true).check();
        assertQuery("SELECT '{\"a\":1}' IS NOT JSON").returns(false).check();
        assertQuery("SELECT '{\"a\":1}' IS NOT JSON VALUE").returns(false).check();
        assertQuery("SELECT '{\"a\":1}' IS NOT JSON OBJECT").returns(false).check();
        assertQuery("SELECT '[1, 2]' IS NOT JSON ARRAY").returns(false).check();
        assertQuery("SELECT '1' IS NOT JSON SCALAR").returns(false).check();
    }

    /** */
    @Test
    public void testCurrentTimeFunctions() {
        // Don't check returned value, only ability to use these functions.
        assertQuery("SELECT CURRENT_TIME").check();
        assertQuery("SELECT CURRENT_TIMESTAMP").check();
        assertQuery("SELECT CURRENT_DATE").check();
        assertQuery("SELECT LOCALTIME").check();
        assertQuery("SELECT LOCALTIMESTAMP").check();
    }
}
