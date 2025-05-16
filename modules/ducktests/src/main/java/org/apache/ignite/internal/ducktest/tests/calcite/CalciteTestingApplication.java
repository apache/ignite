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
package org.apache.ignite.internal.ducktest.tests.calcite;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Period;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/**
 * Calcite engine tests
 */
public class CalciteTestingApplication extends IgniteAwareApplication {
    /**
     * Helper class for testing SQL queries.
     * Contains the query, expected results, and a flag to skip verification.
     */
    private static class QueryTest {
        /** Query. */
        String qry;

        /** Expected results. */
        List<Object> expectedResults;

        /** Skip verification. */
        boolean skipVerification;

        /**
         * @param qry Query.
         * @param expectedResults Expected results.
         */
        QueryTest(String qry, Object... expectedResults) {
            this.qry = qry;
            this.expectedResults = Arrays.asList(expectedResults);
            skipVerification = expectedResults.length == 0;
        }
    }

    /** {@inheritDoc} */
    @Override public void run(JsonNode jsonNode) throws SQLException {
        markInitialized();

        try (Connection conn = thinJdbcDataSource.getConnection()) {

            conn.createStatement().execute("CREATE TABLE IF NOT EXISTS t(val INT)");
            conn.createStatement().execute("DELETE FROM t");
            conn.createStatement().execute("INSERT INTO t VALUES (1)");

            testQueries(conn);

            markFinished();
        }
    }

    /** */
    private void testQueries(Connection conn) throws SQLException {
        List<QueryTest> tests = new ArrayList<>();

        // Comparison
        tests.add(new QueryTest("SELECT 2 > 1 FROM t", true));
        tests.add(new QueryTest("SELECT 2 >= 1 FROM t", true));
        tests.add(new QueryTest("SELECT 2 < 1 FROM t", false));
        tests.add(new QueryTest("SELECT 2 <= 1 FROM t", false));
        tests.add(new QueryTest("SELECT 2 = 1 FROM t", false));
        tests.add(new QueryTest("SELECT 2 <> 1 FROM t", true));
        tests.add(new QueryTest("SELECT 2 BETWEEN 1 AND 3 FROM t", true));
        tests.add(new QueryTest("SELECT 2 NOT BETWEEN 1 AND 3 FROM t", false));

        // Set operations
        tests.add(new QueryTest("SELECT 1 UNION SELECT 2", 1, 2));
        tests.add(new QueryTest("SELECT 1 UNION ALL SELECT 1", 1, 1));
        tests.add(new QueryTest("SELECT 1 EXCEPT SELECT 2", 1));
        tests.add(new QueryTest("SELECT 1 EXCEPT ALL SELECT 2", 1));
        tests.add(new QueryTest("SELECT 1 INTERSECT SELECT 1", 1));
        tests.add(new QueryTest("SELECT 1 INTERSECT ALL SELECT 1", 1));

        // Arithmetic
        tests.add(new QueryTest("SELECT 1 + 2 FROM t", 3));
        tests.add(new QueryTest("SELECT 2 - 1 FROM t", 1));
        tests.add(new QueryTest("SELECT 2 * 3 FROM t", 6));
        tests.add(new QueryTest("SELECT 3 / 2 FROM t", 1));
        tests.add(new QueryTest("SELECT -(1) FROM t", -1));
        tests.add(new QueryTest("SELECT +(1) FROM t", 1));
        tests.add(new QueryTest("SELECT 3 % 2 FROM t", 1));

        // Like
        tests.add(new QueryTest("SELECT 'a' LIKE 'a%' FROM t", true));
        tests.add(new QueryTest("SELECT 'a' NOT LIKE 'a%' FROM t", false));
        tests.add(new QueryTest("SELECT 'a' SIMILAR TO '(a|A)%' FROM t", true));
        tests.add(new QueryTest("SELECT 'A' NOT SIMILAR TO '(a|A)%' FROM t", false));

        // String functions
        tests.add(new QueryTest("SELECT UPPER('aA') FROM t", "AA"));
        tests.add(new QueryTest("SELECT LOWER('aA') FROM t", "aa"));
        tests.add(new QueryTest("SELECT INITCAP('aA') FROM t", "Aa"));
        tests.add(new QueryTest("SELECT TO_BASE64('aA') FROM t", "YUE="));
        tests.add(new QueryTest("SELECT FROM_BASE64('YUE=')::VARCHAR FROM t", "aA"));
        tests.add(new QueryTest("SELECT MD5('aa') FROM t", "4124bc0a9335c27f086f24ba207a4912"));
        tests.add(new QueryTest("SELECT SHA1('aa') FROM t", "e0c9035898dd52fc65c41454cec9c4d2611bfb37"));
        tests.add(new QueryTest("SELECT SUBSTRING('aAaA', 2, 2) FROM t", "Aa"));
        tests.add(new QueryTest("SELECT LEFT('aA', 1) FROM t", "a"));
        tests.add(new QueryTest("SELECT RIGHT('aA', 1) FROM t", "A"));
        tests.add(new QueryTest("SELECT REPLACE('aA', 'A', 'a') FROM t", "aa"));
        tests.add(new QueryTest("SELECT TRANSLATE('aA', 'A', 'a') FROM t", "aa"));
        tests.add(new QueryTest("SELECT CHR(97) FROM t", "a"));
        tests.add(new QueryTest("SELECT CHAR_LENGTH('aa') FROM t", 2));
        tests.add(new QueryTest("SELECT CHARACTER_LENGTH('aa') FROM t", 2));
        tests.add(new QueryTest("SELECT 'a' || 'a' FROM t", "aa"));
        tests.add(new QueryTest("SELECT CONCAT('a', 'a') FROM t", "aa"));
        tests.add(new QueryTest("SELECT OVERLAY('aAaA' PLACING 'aA' FROM 2) FROM t", "aaAA"));
        tests.add(new QueryTest("SELECT POSITION('A' IN 'aA') FROM t", 2));
        tests.add(new QueryTest("SELECT ASCII('a') FROM t", 97));
        tests.add(new QueryTest("SELECT REPEAT('a', 2) FROM t", "aa"));
        tests.add(new QueryTest("SELECT SPACE(2) FROM t", "  "));
        tests.add(new QueryTest("SELECT STRCMP('a', 'b') FROM t", 1));
        tests.add(new QueryTest("SELECT SOUNDEX('a') FROM t", "A000"));
        tests.add(new QueryTest("SELECT DIFFERENCE('a', 'A') FROM t", 4));
        tests.add(new QueryTest("SELECT REVERSE('aA') FROM t", "Aa"));
        tests.add(new QueryTest("SELECT TRIM('a' FROM 'aA') FROM t", "A"));
        tests.add(new QueryTest("SELECT LTRIM(' a ') FROM t", "a "));
        tests.add(new QueryTest("SELECT RTRIM(' a ') FROM t", " a"));

        // IS
        tests.add(new QueryTest("SELECT 'a' IS NULL FROM t", false));
        tests.add(new QueryTest("SELECT 'a' IS NOT NULL FROM t", true));
        tests.add(new QueryTest("SELECT 1=1 IS TRUE FROM t", true));
        tests.add(new QueryTest("SELECT 1=1 IS NOT TRUE FROM t", false));
        tests.add(new QueryTest("SELECT 1=1 IS FALSE FROM t", false));
        tests.add(new QueryTest("SELECT 1=1 IS NOT FALSE FROM t", true));
        tests.add(new QueryTest("SELECT NULL IS DISTINCT FROM NULL FROM t", false));
        tests.add(new QueryTest("SELECT NULL IS NOT DISTINCT FROM NULL FROM t", true));

        // Logical
        tests.add(new QueryTest("SELECT FALSE AND TRUE FROM t", false));
        tests.add(new QueryTest("SELECT FALSE OR TRUE FROM t", true));
        tests.add(new QueryTest("SELECT NOT FALSE FROM t", true));

        // Aggregates
        tests.add(new QueryTest("SELECT COUNT(*) FROM t", 1L));
        tests.add(new QueryTest("SELECT SUM(val) FROM t", 1L));
        tests.add(new QueryTest("SELECT AVG(val) FROM t", 1));
        tests.add(new QueryTest("SELECT MIN(val) FROM t", 1));
        tests.add(new QueryTest("SELECT MAX(val) FROM t", 1));
        tests.add(new QueryTest("SELECT COUNT(*) FILTER(WHERE val <> 1) FROM t", 0L));
        tests.add(new QueryTest("SELECT LISTAGG(val, ',') WITHIN GROUP (ORDER BY val DESC) FROM t", "1"));
        tests.add(new QueryTest("SELECT GROUP_CONCAT(val, ',' ORDER BY val DESC) FROM t", "1"));
        tests.add(new QueryTest("SELECT STRING_AGG(val, ',' ORDER BY val DESC) FROM t", "1"));
        tests.add(new QueryTest("SELECT EVERY(val = 1) FROM t", true));
        tests.add(new QueryTest("SELECT SOME(val = 1) FROM t", true));

        // Regex
        tests.add(new QueryTest("SELECT 'aA' ~ '.*aa.*' FROM t", false));
        tests.add(new QueryTest("SELECT 'aA' ~* '.*aa.*' FROM t", true));
        tests.add(new QueryTest("SELECT 'aA' !~ '.*aa.*' FROM t", true));
        tests.add(new QueryTest("SELECT 'aA' !~* '.*aa.*' FROM t", false));
        tests.add(new QueryTest("SELECT REGEXP_REPLACE('aA', '[Aa]+', 'X') FROM t", "X"));

        // Other
        tests.add(new QueryTest("SELECT * FROM (VALUES ROW('a', 1))", "a", 1));
        tests.add(new QueryTest("SELECT CAST('1' AS INT) FROM t", 1));
        tests.add(new QueryTest("SELECT '1'::INT FROM t", 1));
        tests.add(new QueryTest("SELECT COALESCE(null, 'a', 'A') FROM t", "a"));
        tests.add(new QueryTest("SELECT NVL(null, 'a') FROM t", "a"));
        tests.add(new QueryTest("SELECT NULLIF(1, 2) FROM t", 1));
        tests.add(new QueryTest("SELECT CASE WHEN 1=1 THEN 1 ELSE 2 END FROM t", 1));
        tests.add(new QueryTest("SELECT DECODE(1, 1, 1, 2) FROM t", 1));
        tests.add(new QueryTest("SELECT LEAST('a', 'b') FROM t", "a"));
        tests.add(new QueryTest("SELECT GREATEST('a', 'b') FROM t", "b"));
        tests.add(new QueryTest("SELECT COMPRESS('')::VARCHAR FROM t", ""));
        tests.add(new QueryTest("SELECT OCTET_LENGTH(x'01') FROM t", 1));
        tests.add(new QueryTest("SELECT CAST(INTERVAL 1 SECONDS AS INT) FROM t", 1));

        // Multiple columns
        tests.add(new QueryTest("SELECT 1 as col1, 'text' as col2 FROM t", 1, "text"));

        // Date and time
        tests.add(new QueryTest("SELECT DATE '2021-01-01' + interval (1) days FROM t", Date.valueOf("2021-01-02")));
        tests.add(new QueryTest("SELECT (DATE '2021-03-01' - DATE '2021-01-01') months FROM t", Period.ofMonths(2)));
        tests.add(new QueryTest("SELECT EXTRACT(DAY FROM DATE '2021-01-15') FROM t", 15L));
        tests.add(new QueryTest("SELECT FLOOR(DATE '2021-01-15' TO MONTH) FROM t", Date.valueOf("2021-01-01")));
        tests.add(new QueryTest("SELECT CEIL(DATE '2021-01-15' TO MONTH) FROM t", Date.valueOf("2021-02-01")));
        tests.add(new QueryTest("SELECT TIMESTAMPADD(DAY, 1, TIMESTAMP '2021-01-01') FROM t", Timestamp.valueOf("2021-01-02 00:00:00")));
        tests.add(new QueryTest("SELECT TIMESTAMPDIFF(DAY, TIMESTAMP '2021-01-01', TIMESTAMP '2021-01-02') FROM t", 1));
        tests.add(new QueryTest("SELECT LAST_DAY(DATE '2021-01-01') FROM t", Date.valueOf("2021-01-31")));
        tests.add(new QueryTest("SELECT DAYNAME(DATE '2021-01-01') FROM t", "Friday"));
        tests.add(new QueryTest("SELECT MONTHNAME(DATE '2021-01-01') FROM t", "January"));
        tests.add(new QueryTest("SELECT DAYOFMONTH(DATE '2021-01-01') FROM t", 1L));
        tests.add(new QueryTest("SELECT DAYOFWEEK(DATE '2021-01-01') FROM t", 6L));
        tests.add(new QueryTest("SELECT DAYOFYEAR(DATE '2021-01-01') FROM t", 1L));
        tests.add(new QueryTest("SELECT YEAR(DATE '2021-01-01') FROM t", 2021L));
        tests.add(new QueryTest("SELECT QUARTER(DATE '2021-01-01') FROM t", 1L));
        tests.add(new QueryTest("SELECT MONTH(DATE '2021-01-01') FROM t", 1L));
        tests.add(new QueryTest("SELECT WEEK(DATE '2021-01-04') FROM t", 1L));
        tests.add(new QueryTest("SELECT HOUR(TIMESTAMP '2021-01-01 01:01:01') FROM t", 1L));
        tests.add(new QueryTest("SELECT MINUTE(TIMESTAMP '2021-01-01 01:01:01') FROM t", 1L));
        tests.add(new QueryTest("SELECT SECOND(TIMESTAMP '2021-01-01 01:01:01') FROM t", 1L));
        tests.add(new QueryTest("SELECT TIMESTAMP_SECONDS(1609459200) FROM t", Timestamp.valueOf("2021-01-01 00:00:00")));
        tests.add(new QueryTest("SELECT TIMESTAMP_MILLIS(1609459200000) FROM t", Timestamp.valueOf("2021-01-01 00:00:00")));
        tests.add(new QueryTest("SELECT TIMESTAMP_MICROS(1609459200000000) FROM t", Timestamp.valueOf("2021-01-01 00:00:00")));
        tests.add(new QueryTest("SELECT UNIX_SECONDS(TIMESTAMP '2021-01-01 00:00:00') FROM t", 1609459200L));
        tests.add(new QueryTest("SELECT UNIX_MILLIS(TIMESTAMP '2021-01-01 00:00:00') FROM t", 1609459200000L));
        tests.add(new QueryTest("SELECT UNIX_MICROS(TIMESTAMP '2021-01-01 00:00:00') FROM t", 1609459200000000L));
        tests.add(new QueryTest("SELECT UNIX_DATE(DATE '2021-01-01') FROM t", 18628));
        tests.add(new QueryTest("SELECT DATE_FROM_UNIX_DATE(18628) FROM t", Date.valueOf("2021-01-01")));
        tests.add(new QueryTest("SELECT DATE('2021-01-01') FROM t", Date.valueOf("2021-01-01")));
        tests.add(new QueryTest("SELECT TIME(1, 10, 30) FROM t", Time.valueOf("01:10:30")));
        tests.add(new QueryTest("SELECT DATETIME(2021, 1, 1, 1, 10, 30) FROM t", Timestamp.valueOf("2021-01-01 01:10:30")));
        tests.add(new QueryTest("SELECT TO_CHAR(DATE '2021-01-01', 'YYMMDD') FROM t", "210101"));
        tests.add(new QueryTest("SELECT TO_DATE('210101', 'YYMMDD') FROM t", Date.valueOf("2021-01-01")));
        tests.add(new QueryTest("SELECT TO_TIMESTAMP('210101-01-10-30', 'YYMMDD-HH24-MI-SS') FROM t",
            Timestamp.valueOf("2021-01-01 01:10:30")));

        // Math
        tests.add(new QueryTest("SELECT MOD(3, 2) FROM t", 1));
        tests.add(new QueryTest("SELECT EXP(2) FROM t", Math.exp(2)));
        tests.add(new QueryTest("SELECT POWER(2, 2) FROM t", Math.pow(2, 2)));
        tests.add(new QueryTest("SELECT LN(2) FROM t", Math.log(2)));
        tests.add(new QueryTest("SELECT LOG10(2) FROM t", Math.log(2) / Math.log(10)));
        tests.add(new QueryTest("SELECT ABS(-1) FROM t", Math.abs(-1)));
        tests.add(new QueryTest("SELECT RAND() FROM t"));
        tests.add(new QueryTest("SELECT RAND_INTEGER(10) FROM t"));
        tests.add(new QueryTest("SELECT ACOS(1) FROM t", Math.acos(1)));
        tests.add(new QueryTest("SELECT ACOSH(1) FROM t", 0d));
        tests.add(new QueryTest("SELECT ASIN(1) FROM t", Math.asin(1)));
        tests.add(new QueryTest("SELECT ASINH(0) FROM t", 0d));
        tests.add(new QueryTest("SELECT ATAN(1) FROM t", Math.atan(1)));
        tests.add(new QueryTest("SELECT ATANH(0) FROM t", 0d));
        tests.add(new QueryTest("SELECT ATAN2(1, 1) FROM t", Math.atan2(1, 1)));
        tests.add(new QueryTest("SELECT SQRT(4) FROM t", Math.sqrt(4)));
        tests.add(new QueryTest("SELECT CBRT(8) FROM t", Math.cbrt(8)));
        tests.add(new QueryTest("SELECT COS(1) FROM t", Math.cos(1)));
        tests.add(new QueryTest("SELECT COSH(1) FROM t", Math.cosh(1)));
        tests.add(new QueryTest("SELECT COT(1) FROM t", 1.0d / Math.tan(1)));
        tests.add(new QueryTest("SELECT COTH(1) FROM t", 1.0d / Math.tanh(1)));
        tests.add(new QueryTest("SELECT DEGREES(1) FROM t", Math.toDegrees(1)));
        tests.add(new QueryTest("SELECT RADIANS(1) FROM t", Math.toRadians(1)));
        tests.add(new QueryTest("SELECT ROUND(1.7) FROM t", BigDecimal.valueOf(2)));
        tests.add(new QueryTest("SELECT SIGN(-5) FROM t", -1));
        tests.add(new QueryTest("SELECT SIN(1) FROM t", Math.sin(1)));
        tests.add(new QueryTest("SELECT SINH(1) FROM t", Math.sinh(1)));
        tests.add(new QueryTest("SELECT TAN(1) FROM t", Math.tan(1)));
        tests.add(new QueryTest("SELECT TANH(1) FROM t", Math.tanh(1)));
        tests.add(new QueryTest("SELECT SEC(1) FROM t", 1d / Math.cos(1)));
        tests.add(new QueryTest("SELECT SECH(1) FROM t", 1d / Math.cosh(1)));
        tests.add(new QueryTest("SELECT CSC(1) FROM t", 1d / Math.sin(1)));
        tests.add(new QueryTest("SELECT CSCH(1) FROM t", 1d / Math.sinh(1)));
        tests.add(new QueryTest("SELECT TRUNCATE(1.7) FROM t", BigDecimal.valueOf(1)));
        tests.add(new QueryTest("SELECT PI FROM t", Math.PI));

        // Collections
        tests.add(new QueryTest("SELECT ARRAY(SELECT 1) FROM t", Collections.singletonList(1)));
        tests.add(new QueryTest("SELECT ARRAY[1, 2, 3] FROM t", Arrays.asList(1, 2, 3)));
        tests.add(new QueryTest("SELECT ARRAY[1, 2, 3][2] FROM t", 2));
        tests.add(new QueryTest("SELECT CARDINALITY(ARRAY[1, 2, 3]) FROM t", 3));
        tests.add(new QueryTest("SELECT ARRAY[1, 2, 3] IS EMPTY FROM t", false));
        tests.add(new QueryTest("SELECT ARRAY[1, 2, 3] IS NOT EMPTY FROM t", true));

        // JSON
        tests.add(new QueryTest("SELECT '{\"a\":1}' FORMAT JSON FROM t"));
        tests.add(new QueryTest("SELECT JSON_VALUE('{\"a\":1}', '$.a') FROM t", "1"));
        tests.add(new QueryTest("SELECT JSON_VALUE('{\"a\":1}' FORMAT JSON, '$.a') FROM t", "1"));
        tests.add(new QueryTest("SELECT JSON_QUERY('{\"a\":{\"b\":1}}', '$.a') FROM t", "{\"b\":1}"));
        tests.add(new QueryTest("SELECT JSON_TYPE('{\"a\":1}') FROM t", "OBJECT"));
        tests.add(new QueryTest("SELECT JSON_EXISTS('{\"a\":1}', '$.a') FROM t", true));
        tests.add(new QueryTest("SELECT JSON_DEPTH('{\"a\":1}') FROM t", 2));
        tests.add(new QueryTest("SELECT JSON_KEYS('{\"a\":1}') FROM t", "[\"a\"]"));
        tests.add(new QueryTest("SELECT JSON_PRETTY('{\"a\":1}') FROM t", "{\n  \"a\" : 1\n}"));
        tests.add(new QueryTest("SELECT JSON_LENGTH('{\"a\":1}') FROM t", 1));
        tests.add(new QueryTest("SELECT JSON_REMOVE('{\"a\":1, \"b\":2}', '$.a') FROM t", "{\"b\":2}"));
        tests.add(new QueryTest("SELECT JSON_STORAGE_SIZE('1') FROM t", 1));
        tests.add(new QueryTest("SELECT JSON_OBJECT('a': 1) FROM t", "{\"a\":1}"));
        tests.add(new QueryTest("SELECT JSON_ARRAY('a', 'b') FROM t", "[\"a\",\"b\"]"));
        tests.add(new QueryTest("SELECT '{\"a\":1}' IS JSON FROM t", true));
        tests.add(new QueryTest("SELECT '{\"a\":1}' IS JSON VALUE FROM t", true));
        tests.add(new QueryTest("SELECT '{\"a\":1}' IS JSON OBJECT FROM t", true));
        tests.add(new QueryTest("SELECT '[1, 2]' IS JSON ARRAY FROM t", true));
        tests.add(new QueryTest("SELECT '1' IS JSON SCALAR FROM t", true));
        tests.add(new QueryTest("SELECT '{\"a\":1}' IS NOT JSON FROM t", false));
        tests.add(new QueryTest("SELECT '{\"a\":1}' IS NOT JSON VALUE FROM t", false));
        tests.add(new QueryTest("SELECT '{\"a\":1}' IS NOT JSON OBJECT FROM t", false));
        tests.add(new QueryTest("SELECT '[1, 2]' IS NOT JSON ARRAY FROM t", false));
        tests.add(new QueryTest("SELECT '1' IS NOT JSON SCALAR FROM t", false));

        // XML
        tests.add(new QueryTest("SELECT EXTRACTVALUE('<a>b</a>', '//a') FROM t", "b"));
        tests.add(new QueryTest(
            "SELECT XMLTRANSFORM('<a>b</a>','" +
                "<?xml version=\"1.0\"?>" +
                "<xsl:stylesheet version=\"1.0\" " +
                "xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">  " +
                "<xsl:output method=\"text\"/>  " +
                "<xsl:template match=\"/\">    " +
                "a - <xsl:value-of select=\"/a\"/>  " +
                "</xsl:template></xsl:stylesheet>') FROM t",
            "    a - b"
        ));
        tests.add(new QueryTest("SELECT \"EXTRACT\"('<a><b>c</b></a>', '/a/b') FROM t", "<b>c</b>"));
        tests.add(new QueryTest("SELECT EXISTSNODE('<a><b>c</b></a>', '/a/b') FROM t", 1));

        // Currrent time functions
        tests.add(new QueryTest("SELECT CURRENT_TIME FROM t"));
        tests.add(new QueryTest("SELECT CURRENT_TIMESTAMP FROM t"));
        tests.add(new QueryTest("SELECT CURRENT_DATE FROM t"));
        tests.add(new QueryTest("SELECT LOCALTIME FROM t"));
        tests.add(new QueryTest("SELECT LOCALTIMESTAMP FROM t"));

        // Execute all tests
        for (QueryTest test : tests) {
            try (PreparedStatement stmt = conn.prepareStatement(test.qry);
                 ResultSet rs = stmt.executeQuery()) {

                if (test.skipVerification)
                    continue;

                List<Object> actualResults = new ArrayList<>();

                int colCnt = rs.getMetaData().getColumnCount();

                while (rs.next()) {
                    for (int i = 1; i <= colCnt; i++)
                        actualResults.add(rs.getObject(i));
                }

                if (!Objects.equals(test.expectedResults, actualResults)) {
                    String errorMsg = String.format(
                        "Query failed: %s Expected: %s Actual: %s",
                        test.qry, test.expectedResults, actualResults);
                    throw new RuntimeException(errorMsg);

                }
            }
        }
    }
}
