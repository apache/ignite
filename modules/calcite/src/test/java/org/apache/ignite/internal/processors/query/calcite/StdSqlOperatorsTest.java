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
import java.util.Arrays;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.sql.fun.IgniteStdSqlOperatorTable;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Ignite's SQL dialect test.
 * This test contains basic checks for standard SQL operators (only syntax check and check ability to use it).
 *
 * @see IgniteStdSqlOperatorTable
 */
public class StdSqlOperatorsTest extends GridCommonAbstractTest {
    /** */
    private static QueryEngine qryEngine;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        IgniteEx grid = startGrid();

        qryEngine = Commons.lookupComponent(grid.context(), QueryEngine.class);
    }

    /** */
    @Test
    public void testSet() {
        checkQuery("SELECT 1 UNION SELECT 2").returns(1).returns(2).check();
        checkQuery("SELECT 1 UNION ALL SELECT 1").returns(1).returns(1).check();
        checkQuery("SELECT 1 EXCEPT SELECT 2").returns(1).check();
        checkQuery("SELECT 1 EXCEPT ALL SELECT 2").returns(1).check();
        checkQuery("SELECT 1 INTERSECT SELECT 1").returns(1).check();
        checkQuery("SELECT 1 INTERSECT ALL SELECT 1").returns(1).check();
    }

    /** */
    @Test
    public void testLogical() {
        checkQuery("SELECT FALSE AND TRUE").returns(false).check();
        checkQuery("SELECT FALSE OR TRUE").returns(true).check();
        checkQuery("SELECT NOT FALSE").returns(true).check();
    }

    /** */
    @Test
    public void testComparison() {
        checkQuery("SELECT 2 > 1").returns(true).check();
        checkQuery("SELECT 2 >= 1").returns(true).check();
        checkQuery("SELECT 2 < 1").returns(false).check();
        checkQuery("SELECT 2 <= 1").returns(false).check();
        checkQuery("SELECT 2 = 1").returns(false).check();
        checkQuery("SELECT 2 <> 1").returns(true).check();
        checkQuery("SELECT 2 BETWEEN 1 AND 3").returns(true).check();
        checkQuery("SELECT 2 NOT BETWEEN 1 AND 3").returns(false).check();
    }

    /** */
    @Test
    public void testArithmetic() {
        checkQuery("SELECT 1 + 2").returns(3).check();
        checkQuery("SELECT 2 - 1").returns(1).check();
        checkQuery("SELECT 2 * 3").returns(6).check();
        checkQuery("SELECT 3 / 2 ").returns(1).check();
        checkQuery("SELECT -(1)").returns(-1).check();
        checkQuery("SELECT +(1)").returns(1).check();
        checkQuery("SELECT 3 % 2").returns(1).check();
    }

    /** */
    @Test
    public void testAggregates() {
        checkQuery("SELECT COUNT(*) FROM (SELECT 1 AS a)").returns(1L).check();
        checkQuery("SELECT SUM(a) FROM (SELECT 1 AS a)").returns(1).check();
        checkQuery("SELECT AVG(a) FROM (SELECT 1 AS a)").returns(1).check();
        checkQuery("SELECT MIN(a) FROM (SELECT 1 AS a)").returns(1).check();
        checkQuery("SELECT MAX(a) FROM (SELECT 1 AS a)").returns(1).check();
        checkQuery("SELECT ANY_VALUE(a) FROM (SELECT 1 AS a)").returns(1).check();
        checkQuery("SELECT COUNT(*) FILTER(WHERE a <> 1) FROM (SELECT 1 AS a)").returns(0L).check();
    }

    /** */
    @Test
    public void testIs() {
        checkQuery("SELECT 'a' IS NULL").returns(false).check();
        checkQuery("SELECT 'a' IS NOT NULL").returns(true).check();
        checkQuery("SELECT 1=1 IS TRUE").returns(true).check();
        checkQuery("SELECT 1=1 IS NOT TRUE").returns(false).check();
        checkQuery("SELECT 1=1 IS FALSE").returns(false).check();
        checkQuery("SELECT 1=1 IS NOT FALSE").returns(true).check();
    }

    /** */
    @Test
    public void testLike() {
        checkQuery("SELECT 'a' LIKE 'a%'").returns(true).check();
        checkQuery("SELECT 'a' NOT LIKE 'a%'").returns(false).check();
        checkQuery("SELECT 'a' SIMILAR TO '(a|A)%'").returns(true).check();
        checkQuery("SELECT 'A' NOT SIMILAR TO '(a|A)%'").returns(false).check();
    }

    /** */
    @Test
    public void testNullsOrdering() {
        checkQuery("SELECT a FROM (SELECT 1 AS a) ORDER BY a NULLS FIRST").returns(1).check();
        checkQuery("SELECT a FROM (SELECT 1 AS a) ORDER BY a NULLS LAST").returns(1).check();
        checkQuery("SELECT a FROM (SELECT 1 AS a) ORDER BY a DESC NULLS FIRST").returns(1).check();
        checkQuery("SELECT a FROM (SELECT 1 AS a) ORDER BY a DESC NULLS LAST").returns(1).check();
        checkQuery("SELECT a FROM (SELECT 1 AS a) ORDER BY a ASC NULLS FIRST").returns(1).check();
        checkQuery("SELECT a FROM (SELECT 1 AS a) ORDER BY a ASC NULLS LAST").returns(1).check();
    }

    /** */
    @Test
    public void testExists() {
        checkQuery("SELECT EXISTS (SELECT 1)").returns(true).check();
        checkQuery("SELECT NOT EXISTS (SELECT 1)").returns(false).check();
    }

    /** */
    @Test
    public void testStringFunctions() {
        checkQuery("SELECT UPPER('aA')").returns("AA").check();
        checkQuery("SELECT LOWER('aA')").returns("aa").check();
        checkQuery("SELECT INITCAP('aA')").returns("Aa").check();
        checkQuery("SELECT TO_BASE64('aA')").returns("YUE=").check();
        checkQuery("SELECT FROM_BASE64('YUE=')").returns(new ByteString(new byte[] {'a', 'A'})).check();
        checkQuery("SELECT MD5('aa')").returns("4124bc0a9335c27f086f24ba207a4912").check();
        checkQuery("SELECT SHA1('aa')").returns("e0c9035898dd52fc65c41454cec9c4d2611bfb37").check();
        checkQuery("SELECT SUBSTRING('aAaA', 2, 2)").returns("Aa").check();
        checkQuery("SELECT LEFT('aA', 1)").returns("a").check();
        checkQuery("SELECT RIGHT('aA', 1)").returns("A").check();
        checkQuery("SELECT REPLACE('aA', 'A', 'a')").returns("aa").check();
        checkQuery("SELECT TRANSLATE('aA', 'A', 'a')").returns("aa").check();
        checkQuery("SELECT CHR(97)").returns("a").check();
        checkQuery("SELECT CHAR_LENGTH('aa')").returns(2).check();
        checkQuery("SELECT CHARACTER_LENGTH('aa')").returns(2).check();
        checkQuery("SELECT 'a' || 'a'").returns("aa").check();
        checkQuery("SELECT CONCAT('a', 'a')").returns("aa").check();
        checkQuery("SELECT OVERLAY('aAaA' PLACING 'aA' FROM 2)").returns("aaAA").check();
        checkQuery("SELECT POSITION('A' IN 'aA')").returns(2).check();
        checkQuery("SELECT ASCII('a')").returns(97).check();
        checkQuery("SELECT REPEAT('a', 2)").returns("aa").check();
        checkQuery("SELECT SPACE(2)").returns("  ").check();
        checkQuery("SELECT STRCMP('a', 'b')").returns(1).check();
        checkQuery("SELECT SOUNDEX('a')").returns("A000").check();
        checkQuery("SELECT DIFFERENCE('a', 'A')").returns(4).check();
        checkQuery("SELECT REVERSE('aA')").returns("Aa").check();
        checkQuery("SELECT TRIM('a' FROM 'aA')").returns("A").check();
        checkQuery("SELECT LTRIM(' a ')").returns("a ").check();
        checkQuery("SELECT RTRIM(' a ')").returns(" a").check();
    }

    /** */
    @Test
    public void testMathFunctions() {
        checkQuery("SELECT MOD(3, 2)").returns(1).check();
        checkQuery("SELECT EXP(2)").returns(Math.exp(2)).check();
        checkQuery("SELECT POWER(2, 2)").returns(Math.pow(2, 2)).check();
        checkQuery("SELECT LN(2)").returns(Math.log(2)).check();
        checkQuery("SELECT LOG10(2) ").returns(Math.log10(2)).check();
        checkQuery("SELECT ABS(-1)").returns(Math.abs(-1)).check();
        checkQuery("SELECT RAND()").check();
        checkQuery("SELECT RAND_INTEGER(10)").check();
        checkQuery("SELECT ACOS(1)").returns(Math.acos(1)).check();
        checkQuery("SELECT ASIN(1)").returns(Math.asin(1)).check();
        checkQuery("SELECT ATAN(1)").returns(Math.atan(1)).check();
        checkQuery("SELECT ATAN2(1, 1)").returns(Math.atan2(1, 1)).check();
        checkQuery("SELECT SQRT(4)").returns(Math.sqrt(4)).check();
        checkQuery("SELECT CBRT(8)").returns(Math.cbrt(8)).check();
        checkQuery("SELECT COS(1)").returns(Math.cos(1)).check();
        checkQuery("SELECT COSH(1)").returns(Math.cosh(1)).check();
        checkQuery("SELECT COT(1)").returns(1.0d / Math.tan(1)).check();
        checkQuery("SELECT DEGREES(1)").returns(Math.toDegrees(1)).check();
        checkQuery("SELECT RADIANS(1)").returns(Math.toRadians(1)).check();
        checkQuery("SELECT ROUND(1.7)").returns(BigDecimal.valueOf(2)).check();
        checkQuery("SELECT SIGN(-5)").returns(-1).check();
        checkQuery("SELECT SIN(1)").returns(Math.sin(1)).check();
        checkQuery("SELECT SINH(1)").returns(Math.sinh(1)).check();
        checkQuery("SELECT TAN(1)").returns(Math.tan(1)).check();
        checkQuery("SELECT TANH(1)").returns(Math.tanh(1)).check();
        checkQuery("SELECT TRUNCATE(1.7)").returns(BigDecimal.valueOf(1)).check();
        checkQuery("SELECT PI").returns(Math.PI).check();
    }

    /** */
    @Test
    public void testDateAndTime() {
        checkQuery("SELECT DATE '2021-01-01' + interval (1) days").returns(Date.valueOf("2021-01-02")).check();
        checkQuery("SELECT (DATE '2021-03-01' - DATE '2021-01-01') months").returns(2).check();
        checkQuery("SELECT EXTRACT(DAY FROM DATE '2021-01-15')").returns(15L).check();
        checkQuery("SELECT FLOOR(DATE '2021-01-15' TO MONTH)").returns(Date.valueOf("2021-01-01")).check();
        checkQuery("SELECT CEIL(DATE '2021-01-15' TO MONTH)").returns(Date.valueOf("2021-02-01")).check();
        checkQuery("SELECT TIMESTAMPADD(DAY, 1, TIMESTAMP '2021-01-01')").returns(Timestamp.valueOf("2021-01-02 00:00:00")).check();
        checkQuery("SELECT TIMESTAMPDIFF(DAY, TIMESTAMP '2021-01-01', TIMESTAMP '2021-01-02')").returns(1).check();
        checkQuery("SELECT LAST_DAY(DATE '2021-01-01')").returns(Date.valueOf("2021-01-31")).check();
        checkQuery("SELECT DAYNAME(DATE '2021-01-01')").returns("Friday").check();
        checkQuery("SELECT MONTHNAME(DATE '2021-01-01')").returns("January").check();
        checkQuery("SELECT DAYOFMONTH(DATE '2021-01-01')").returns(1L).check();
        checkQuery("SELECT DAYOFWEEK(DATE '2021-01-01')").returns(6L).check();
        checkQuery("SELECT DAYOFYEAR(DATE '2021-01-01')").returns(1L).check();
        checkQuery("SELECT YEAR(DATE '2021-01-01')").returns(2021L).check();
        checkQuery("SELECT QUARTER(DATE '2021-01-01')").returns(1L).check();
        checkQuery("SELECT MONTH(DATE '2021-01-01')").returns(1L).check();
        checkQuery("SELECT WEEK(DATE '2021-01-04')").returns(1L).check();
        checkQuery("SELECT HOUR(TIMESTAMP '2021-01-01 01:01:01')").returns(1L).check();
        checkQuery("SELECT MINUTE(TIMESTAMP '2021-01-01 01:01:01')").returns(1L).check();
        checkQuery("SELECT SECOND(TIMESTAMP '2021-01-01 01:01:01')").returns(1L).check();
        checkQuery("SELECT TIMESTAMP_SECONDS(1609459200)").returns(Timestamp.valueOf("2021-01-01 00:00:00")).check();
        checkQuery("SELECT TIMESTAMP_MILLIS(1609459200000)").returns(Timestamp.valueOf("2021-01-01 00:00:00")).check();
        checkQuery("SELECT TIMESTAMP_MICROS(1609459200000000)").returns(Timestamp.valueOf("2021-01-01 00:00:00")).check();
        checkQuery("SELECT UNIX_SECONDS(TIMESTAMP '2021-01-01 00:00:00')").returns(1609459200L).check();
        checkQuery("SELECT UNIX_MILLIS(TIMESTAMP '2021-01-01 00:00:00')").returns(1609459200000L).check();
        checkQuery("SELECT UNIX_MICROS(TIMESTAMP '2021-01-01 00:00:00')").returns(1609459200000000L).check();
        checkQuery("SELECT UNIX_DATE(DATE '2021-01-01')").returns(18628).check();
        checkQuery("SELECT DATE_FROM_UNIX_DATE(18628)").returns(Date.valueOf("2021-01-01")).check();
        checkQuery("SELECT DATE('2021-01-01')").returns(Date.valueOf("2021-01-01")).check();
        //checkQuery("SELECT TO_DATE('20210101', 'yyyymmdd')").returns(Date.valueOf("2021-01-01")).check();
    }

    /** */
    @Test
    public void testPosixRegex() {
        checkQuery("SELECT 'aA' ~ '.*aa.*'").returns(false).check();
        checkQuery("SELECT 'aA' ~* '.*aa.*'").returns(true).check();
        checkQuery("SELECT 'aA' !~ '.*aa.*'").returns(true).check();
        checkQuery("SELECT 'aA' !~* '.*aa.*'").returns(false).check();
        checkQuery("SELECT REGEXP_REPLACE('aA', '[Aa]+', 'X')").returns("X").check();
    }

    /** */
    @Test
    public void testCollections() {
        checkQuery("SELECT MAP['a', 1, 'A', 2]").returns(F.asMap("a", 1, "A", 2)).check();
        checkQuery("SELECT ARRAY[1, 2, 3]").returns(Arrays.asList(1, 2, 3)).check();
        checkQuery("SELECT ARRAY[1, 2, 3][2]").returns(2).check();
        checkQuery("SELECT CARDINALITY(ARRAY[1, 2, 3])").returns(3).check();
        checkQuery("SELECT ARRAY[1, 2, 3] IS EMPTY").returns(false).check();
        checkQuery("SELECT ARRAY[1, 2, 3] IS NOT EMPTY").returns(true).check();

/*
        checkQuery("SELECT MULTISET[1, 2, 3]").check();
        checkQuery("SELECT MAP(SELECT 'a', 1)").returns(F.asMap("a", 1)).check();
        checkQuery("SELECT ARRAY(SELECT 1)").returns(Arrays.asList(1)).check();
*/
    }

    /** */
    @Test
    public void testOtherFunctions() {
        checkQuery("SELECT * FROM (VALUES ROW('a', 1))").returns("a", 1).check();
        checkQuery("SELECT CAST('1' AS INT)").returns(1).check();
        checkQuery("SELECT '1'::INT").returns(1).check();
        checkQuery("SELECT COALESCE(null, 'a', 'A')").returns("a").check();
        checkQuery("SELECT NVL(null, 'a')").returns("a").check();
        checkQuery("SELECT NULLIF(1, 2)").returns(1).check();
        checkQuery("SELECT CASE WHEN 1=1 THEN 1 ELSE 2 END").returns(1).check();
        checkQuery("SELECT DECODE(1, 1, 1, 2)").returns(1).check();
        checkQuery("SELECT LEAST('a', 'b')").returns("a").check();
        checkQuery("SELECT GREATEST('a', 'b')").returns("b").check();
        checkQuery("SELECT COMPRESS('')::VARCHAR").returns("").check();
    }

    /** */
    @Test
    public void testXml() {
        checkQuery("SELECT EXTRACTVALUE('<a>b</a>', '//a')").returns("b").check();
        checkQuery("SELECT XMLTRANSFORM('<a>b</a>',"
            + "'<?xml version=\"1.0\"?>\n"
            + "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">"
            + "  <xsl:output method=\"text\"/>"
            + "  <xsl:template match=\"/\">"
            + "    a - <xsl:value-of select=\"/a\"/>"
            + "  </xsl:template>"
            + "</xsl:stylesheet>')"
        ).returns("    a - b").check();
        checkQuery("SELECT \"EXTRACT\"('<a><b>c</b></a>', '/a/b')").returns("<b>c</b>").check();
        checkQuery("SELECT EXISTSNODE('<a><b>c</b></a>', '/a/b')").returns(1).check();
    }

    /** */
    @Test
    public void testJson() {
        checkQuery("SELECT '{\"a\":1}' FORMAT JSON").check();
        checkQuery("SELECT JSON_VALUE('{\"a\":1}', '$.a')").returns("1").check();
        checkQuery("SELECT JSON_VALUE('{\"a\":1}' FORMAT JSON, '$.a')").returns("1").check();
        checkQuery("SELECT JSON_QUERY('{\"a\":{\"b\":1}}', '$.a')").returns("{\"b\":1}").check();
        checkQuery("SELECT JSON_TYPE('{\"a\":1}')").returns("OBJECT").check();
        checkQuery("SELECT JSON_EXISTS('{\"a\":1}', '$.a')").returns(true).check();
        checkQuery("SELECT JSON_DEPTH('{\"a\":1}')").returns(2).check();
        checkQuery("SELECT JSON_KEYS('{\"a\":1}')").returns("[\"a\"]").check();
        checkQuery("SELECT JSON_PRETTY('{\"a\":1}')").returns("{\n  \"a\" : 1\n}").check();
        checkQuery("SELECT JSON_LENGTH('{\"a\":1}')").returns(1).check();
        checkQuery("SELECT JSON_REMOVE('{\"a\":1, \"b\":2}', '$.a')").returns("{\"b\":2}").check();
        checkQuery("SELECT JSON_STORAGE_SIZE('1')").returns(1).check();
        checkQuery("SELECT JSON_OBJECT('a': 1)").returns("{\"a\":1}").check();
        checkQuery("SELECT JSON_ARRAY('a', 'b')").returns("[\"a\",\"b\"]").check();
        checkQuery("SELECT '{\"a\":1}' IS JSON").returns(true).check();
        checkQuery("SELECT '{\"a\":1}' IS JSON VALUE").returns(true).check();
        checkQuery("SELECT '{\"a\":1}' IS JSON OBJECT").returns(true).check();
        checkQuery("SELECT '[1, 2]' IS JSON ARRAY").returns(true).check();
        checkQuery("SELECT '1' IS JSON SCALAR").returns(true).check();
        checkQuery("SELECT '{\"a\":1}' IS NOT JSON").returns(false).check();
        checkQuery("SELECT '{\"a\":1}' IS NOT JSON VALUE").returns(false).check();
        checkQuery("SELECT '{\"a\":1}' IS NOT JSON OBJECT").returns(false).check();
        checkQuery("SELECT '[1, 2]' IS NOT JSON ARRAY").returns(false).check();
        checkQuery("SELECT '1' IS NOT JSON SCALAR").returns(false).check();
    }

    /** */
    @Test
    public void testSystemFunctions() {
/*
        checkQuery("SELECT USER").check();
        checkQuery("SELECT CURRENT_USER").check();
        checkQuery("SELECT SESSION_USER").check();
        checkQuery("SELECT SYSTEM_USER").check();
*/
        checkQuery("SELECT CURRENT_PATH").check();
        checkQuery("SELECT CURRENT_ROLE").check();
        checkQuery("SELECT CURRENT_CATALOG").check();
    }

    /** */
    @Test
    public void testCurrentTimeFunctions() {
        // Don't check returned value, only ability to use these functions.
        checkQuery("SELECT CURRENT_TIME").check();
        checkQuery("SELECT CURRENT_TIMESTAMP").check();
        checkQuery("SELECT CURRENT_DATE").check();
        checkQuery("SELECT LOCALTIME").check();
        checkQuery("SELECT LOCALTIMESTAMP").check();
    }

    /** */
    private QueryChecker checkQuery(String qry) {
        return new QueryChecker(qry) {
            @Override protected QueryEngine getEngine() {
                return qryEngine;
            }
        };
    }
}
