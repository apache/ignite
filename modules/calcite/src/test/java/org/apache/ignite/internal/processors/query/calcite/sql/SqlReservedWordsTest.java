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

package org.apache.ignite.internal.processors.query.calcite.sql;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.ignite.internal.processors.query.calcite.sql.generated.IgniteSqlParserImpl;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test reserved keywords.
 */
public class SqlReservedWordsTest extends GridCommonAbstractTest {
    /** List of keywords reserved in Ignite SQL. */
    private static final Set<String> RESERVED_WORDS = new HashSet<>(Arrays.asList(
        "ALL", // UNION ALL
        "ALTER",
        "AND",
        "ANY",
        "ARRAY",
        "AS",
        "ASYMMETRIC", // BETWEEN ASYMMETRIC .. AND ..
        "BETWEEN",
        "BOTH", // TRIM(BOTH .. FROM ..)
        "BY", // GROUP BY
        "CASE",
        "CAST",
        "COLUMN",
        "CONSTRAINT",
        "CREATE",
        "CROSS", // CROSS JOIN
        "CURRENT_DATE",
        "CURRENT_SCHEMA",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "CURRENT_USER",
        "DEFAULT",
        "DELETE",
        "DISTINCT",
        "DROP",
        "ELSE",
        "EXCEPT",
        "EXISTS",
        "EXPLAIN",
        "FALSE",
        "FETCH",
        "FOR", // SUBSTRING(.. FROM .. FOR ..)
        "FROM",
        "FULL", // FULL JOIN
        "GROUP",
        "HAVING",
        "IN",
        "INNER",
        "INSERT",
        "INTERVAL",
        "INTERSECT",
        "INTO",
        "IS",
        "JOIN",
        "LEADING", // TRIM(LEADING .. FROM ..)
        "LEFT", // LEFT JOIN
        "LIKE",
        "LIMIT",
        "LOCALTIME",
        "LOCALTIMESTAMP",
        "MERGE",
        "MINUS",
        "NATURAL", // NATURAL JOIN
        "NOT",
        "NULL",
        "OFFSET",
        "ON",
        "OR",
        "ORDER",
        "OUTER", // OUTER JOIN
        "PARTITION",
        "PRIMARY",
        "RIGHT",
        "ROW",
        "SELECT",
        "SET",
        "SOME",
        "SYMMETRIC", // BETWEEN SYMMETRIC .. AND ..
        "TABLE",
        "THEN",
        "TO",
        "TRAILING", // TRIM(TRAILING .. FROM ..)
        "TRUE",
        "UNION",
        "UNNEST",
        "UPDATE",
        "USER",
        "USING",
        "VALUES",
        "WHEN",
        "WHERE",
        "WITH",

        // Keywords added by Ignite.
        "IF",
        "INDEX"
    ));

    /** */
    @Test
    public void testReservedWords() {
        SqlAbstractParserImpl.Metadata md = IgniteSqlParserImpl.FACTORY.getParser(null).getMetadata();

        Set<String> unexpectedReserved = new TreeSet<>();
        Set<String> shouldBeReserved = new TreeSet<>();

        for (String s: md.getTokens()) {
            if (md.isReservedWord(s) && !RESERVED_WORDS.contains(s))
                unexpectedReserved.add(s);
            else if (!md.isReservedWord(s) && RESERVED_WORDS.contains(s))
                shouldBeReserved.add(s);
        }

        assertTrue("Unexpected reserved keywords: \n" + formatKeywords(unexpectedReserved),
            unexpectedReserved.isEmpty());

        assertTrue("Keywords should be reserved: \n" + formatKeywords(shouldBeReserved),
            shouldBeReserved.isEmpty());
    }

    /**
     * Formatted as config.fmpp:nonReservedKeywordsToAdd keywords.
     */
    private String formatKeywords(Set<String> keywords) {
        StringBuilder sb = new StringBuilder();

        keywords.forEach(s -> sb.append("      \"").append(s).append("\"\n"));

        return sb.toString();
    }
}
