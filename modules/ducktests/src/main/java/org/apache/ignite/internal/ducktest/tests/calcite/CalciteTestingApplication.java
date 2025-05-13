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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.ResultSetMetaData;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.math.BigDecimal;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.ignite.internal.ducktest.utils.IgniteAwareApplication;

/** Tests sql queries for calcite engine */
public class CalciteTestingApplication extends IgniteAwareApplication {

    /** */
    private static class QueryTest {
        String query;
        List<Object> expectedResults;
        Class<?> expectedType;

        QueryTest(String query, Object... expectedResults) {
            this.query = query;
            this.expectedResults = Arrays.asList(expectedResults);
            this.expectedType = expectedResults[0].getClass();
        }
    }

    /** */
    @Override
    public void run(JsonNode jsonNode) throws SQLException {
        markInitialized();

        try (Connection conn = thinJdbcDataSource.getConnection()) {
            beforeTest(conn);
            testQueries(conn);
            markFinished();
        }
    }

    /** */
    private void beforeTest(Connection conn) throws SQLException {
        conn.createStatement().execute("CREATE TABLE IF NOT EXISTS t(val INT)");
        conn.createStatement().execute("DELETE FROM t");
        conn.createStatement().execute("INSERT INTO t VALUES (1)");
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
        tests.add(new QueryTest("SELECT COALESCE(null, 'a', 'A') FROM t", "a"));
        tests.add(new QueryTest("SELECT NVL(null, 'a') FROM t", "a"));
        tests.add(new QueryTest("SELECT NULLIF(1, 2) FROM t", 1));
        tests.add(new QueryTest("SELECT CASE WHEN 1=1 THEN 1 ELSE 2 END FROM t", 1));
        tests.add(new QueryTest("SELECT LEAST('a', 'b') FROM t", "a"));
        tests.add(new QueryTest("SELECT GREATEST('a', 'b') FROM t", "b"));

        // Multiple columns
        tests.add(new QueryTest("SELECT 1 as col1, 'text' as col2 FROM t", 1, "text"));

        for (QueryTest test : tests) {
            try (PreparedStatement stmt = conn.prepareStatement(test.query);
                 ResultSet rs = stmt.executeQuery()) {

                List<Object> actualResults = new ArrayList<>();
                ResultSetMetaData meta = rs.getMetaData();
                int colCount = meta.getColumnCount();

                while (rs.next()) {
                    for (int i = 1; i <= colCount; i++) {
                        actualResults.add(rs.getObject(i));
                    }
                }

                if (!compareResults(test.expectedResults, actualResults, test.expectedType)) {
                    String errorMsg = String.format(
                            "Query failed: %s\nExpected: %s\nActual: %s",
                            test.query, test.expectedResults, actualResults);
                    throw new RuntimeException(errorMsg);
                }
            }
        }
    }

    /** */
    private boolean compareResults(List<Object> expected, List<Object> actual, Class<?> expectedType) {
        if (expected.size() != actual.size()) {
            return false;
        }

        for (int i = 0; i < expected.size(); i++) {
            Object exp = expected.get(i);
            Object act = actual.get(i);

            if (exp == null && act == null) continue;
            if (exp == null || act == null) return false;

            if (exp instanceof BigDecimal && act instanceof BigDecimal) {
                if (((BigDecimal)exp).compareTo((BigDecimal)act) != 0) {
                    return false;
                }
            } else if (!exp.equals(act)) {
                return false;
            }
        }

        return true;
    }
}