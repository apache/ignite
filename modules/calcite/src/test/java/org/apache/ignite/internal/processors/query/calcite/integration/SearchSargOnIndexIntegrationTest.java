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

import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

/**
 * Tests of SEARCH/SARG operator.
 */
public class SearchSargOnIndexIntegrationTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        sql("CREATE TABLE test (c1 INTEGER, c2 VARCHAR, c3 INTEGER)");
        sql("CREATE INDEX c1c2c3 ON test(c1, c2, c3)");

        sql("INSERT INTO test VALUES (0, null, 0)");

        for (int i = 0; i <= 5; i++) {
            for (int j = 1; j <= 5; j++)
                sql("INSERT INTO test VALUES (?, ?, ?)", i == 0 ? null : i, Integer.toString(j), i * j);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        // Skip super method to keep caches after each test.
    }

    /** */
    @Test
    public void testIn() {
        assertQuery("SELECT * FROM test WHERE c1 IN (2, 3) AND c2 IN ('2', '3') AND c3 IN (6, 9)")
            .returns(2, "3", 6)
            .returns(3, "2", 6)
            .returns(3, "3", 9)
            .check();

        assertQuery("SELECT * FROM test WHERE (c1 = 2 OR c1 IS NULL) AND c2 IN ('2', '3') AND c3 IN (0, 6)")
            .returns(null, "2", 0)
            .returns(null, "3", 0)
            .returns(2, "3", 6)
            .check();
    }

    /** */
    @Test
    public void testRange() {
        assertQuery("SELECT * FROM test WHERE ((c1 > 1 AND c1 < 3) OR (c1 > 3 AND c1 < 5)) AND c2 > '2' AND c2 < '5'")
            .returns(2, "3", 6)
            .returns(2, "4", 8)
            .returns(4, "3", 12)
            .returns(4, "4", 16)
            .check();

        assertQuery("SELECT * FROM test WHERE c1 IN (1, 2) AND ((c2 >= '2' AND c2 < '3') OR (c2 > '4' AND c1 <= '5'))")
            .returns(1, "2", 2)
            .returns(1, "5", 5)
            .returns(2, "2", 4)
            .returns(2, "5", 10)
            .check();

        assertQuery("SELECT * FROM test WHERE c1 IN (1, 2) AND " +
            "(c2 < '1' OR (c2 >= '2' AND c2 < '3') OR (c2 > '2' AND c2 < '4') OR c2 > '5')")
            .returns(1, "2", 2)
            .returns(1, "3", 3)
            .returns(2, "2", 4)
            .returns(2, "3", 6)
            .check();

        assertQuery("SELECT * FROM test WHERE c1 = 4 AND c2 > '3' AND c3 in (16, 20)")
            .returns(4, "4", 16)
            .returns(4, "5", 20)
            .check();
    }

    /** */
    @Test
    public void testNulls() {
        assertQuery("SELECT * FROM test WHERE c1 IS NULL AND c2 <= '1'")
            .returns(null, "1", 0)
            .check();

        assertQuery("SELECT * FROM test WHERE c1 IS NOT NULL AND c2 IS NULL")
            .returns(0, null, 0)
            .check();
    }

    /** Tests not supported SEARCH/SARG conditions. */
    @Test
    public void testNot() {
        assertQuery("SELECT * FROM test WHERE c1 <> 1 AND c3 = 6")
            .matches(QueryChecker.containsTableScan("PUBLIC", "TEST")) // Can't use index scan.
            .returns(2, "3", 6)
            .returns(3, "2", 6)
            .check();

        assertQuery("SELECT * FROM test WHERE c1 NOT IN (1, 2, 5) AND c2 NOT IN (1, 2, 5)")
            .matches(QueryChecker.containsTableScan("PUBLIC", "TEST")) // Can't use index scan.
            .returns(3, "3", 9)
            .returns(3, "4", 12)
            .returns(4, "3", 12)
            .returns(4, "4", 16)
            .check();
    }

    /** Test correct index ordering without additional sorting. */
    @Test
    public void testOrdering() {
        assertQuery("SELECT * FROM test WHERE c1 IN (3, 2) AND c2 IN ('3', '2') ORDER BY c1, c2, c3")
            .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IgniteSort"))) // Don't require additional sorting.
            .ordered()
            .returns(2, "2", 4)
            .returns(2, "3", 6)
            .returns(3, "2", 6)
            .returns(3, "3", 9)
            .check();

        assertQuery("SELECT * FROM test WHERE c1 IN (2, 3) AND c2 < '3' ORDER BY c1, c2, c3")
            .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IgniteSort"))) // Don't require additional sorting.
            .ordered()
            .returns(2, "1", 2)
            .returns(2, "2", 4)
            .returns(3, "1", 3)
            .returns(3, "2", 6)
            .check();

        assertQuery("SELECT * FROM test WHERE c1 IN (2, 3) AND c2 IN ('2', '3') AND c3 BETWEEN 5 AND 7 " +
            "ORDER BY c1, c2, c3")
            .matches(CoreMatchers.not(QueryChecker.containsSubPlan("IgniteSort"))) // Don't require additional sorting.
            .ordered()
            .returns(2, "3", 6)
            .returns(3, "2", 6)
            .check();
    }
}
