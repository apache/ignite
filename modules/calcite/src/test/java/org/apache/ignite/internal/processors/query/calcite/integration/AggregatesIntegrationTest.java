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

import java.util.List;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 *
 */
public class AggregatesIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    @Test
    public void testCountWithBackupsAndCacheModes() {
        for (int b = 0; b < 2; ++b) {
            createAndPopulateTable(b, CacheMode.PARTITIONED);

            assertQuery("select count(*) from person").returns(5L).check();

            client.destroyCache(TABLE_NAME);
        }

        createAndPopulateTable(0, CacheMode.REPLICATED);

        assertQuery("select count(*) from person").returns(5L).check();
    }

    /** */
    @Test
    public void testCountOfNonNumericField() {
        createAndPopulateTable();

        assertQuery("select count(name) from person").returns(4L).check();
        assertQuery("select count(*) from person").returns(5L).check();
        assertQuery("select count(1) from person").returns(5L).check();
        assertQuery("select count(null) from person").returns(0L).check();

        assertQuery("select count(DISTINCT name) from person").returns(3L).check();
        assertQuery("select count(DISTINCT 1) from person").returns(1L).check();

        assertQuery("select count(*) from person where salary < 0").returns(0L).check();
        assertQuery("select count(*) from person where salary < 0 and salary > 0").returns(0L).check();

        assertQuery("select count(case when name like 'R%' then 1 else null end) from person").returns(2L).check();
        assertQuery("select count(case when name not like 'I%' then 1 else null end) from person").returns(2L).check();

        assertQuery("select count(name) from person where salary > 10").returns(1L).check();
        assertQuery("select count(*) from person where salary > 10").returns(2L).check();
        assertQuery("select count(1) from person where salary > 10").returns(2L).check();
        assertQuery("select count(*) from person where name is not null").returns(4L).check();

        assertQuery("select count(name) filter (where salary > 10) from person").returns(1L).check();
        assertQuery("select count(*) filter (where salary > 10) from person").returns(2L).check();
        assertQuery("select count(1) filter (where salary > 10) from person").returns(2L).check();

        assertQuery("select salary, count(name) from person group by salary order by salary")
            .returns(10d, 3L)
            .returns(15d, 1L)
            .check();

        assertQuery("select salary, count(*) from person group by salary order by salary")
            .returns(10d, 3L)
            .returns(15d, 2L)
            .check();

        assertQuery("select salary, count(1) from person group by salary order by salary")
            .returns(10d, 3L)
            .returns(15d, 2L)
            .check();

        assertQuery("select salary, count(1), sum(1) from person group by salary order by salary")
            .returns(10d, 3L, 3L)
            .returns(15d, 2L, 2L)
            .check();

        assertQuery("select salary, name, count(1), sum(salary) from person group by salary, name order by salary")
            .returns(10d, "Igor", 1L, 10d)
            .returns(10d, "Roma", 2L, 20d)
            .returns(15d, "Ilya", 1L, 15d)
            .returns(15d, null, 1L, 15d)
            .check();

        assertQuery("select salary, count(name) from person group by salary having salary < 10 order by salary")
            .check();

        assertQuery("select count(_key), _key from person group by _key")
            .returns(1L, 0)
            .returns(1L, 1)
            .returns(1L, 2)
            .returns(1L, 3)
            .returns(1L, 4)
            .check();

        assertQuery("select count(name), name from person group by name")
            .returns(1L, "Igor")
            .returns(1L, "Ilya")
            .returns(2L, "Roma")
            .returns(0L, null)
            .check();

        assertQuery("select avg(salary) from person")
            .returns(12.0)
            .check();
    }

    /** */
    @SuppressWarnings("ThrowableNotThrown")
    @Test
    public void testMultipleRowsFromSingleAggr() throws IgniteCheckedException {
        createAndPopulateTable();

        GridTestUtils.assertThrowsWithCause(() -> assertQuery("SELECT (SELECT name FROM person)").check(),
            IllegalArgumentException.class);

        GridTestUtils.assertThrowsWithCause(() -> assertQuery("SELECT t._key, (SELECT x FROM " +
                "TABLE(system_range(1, 5))) FROM person t").check(), IllegalArgumentException.class);

        GridTestUtils.assertThrowsWithCause(() -> assertQuery("SELECT t._key, (SELECT x FROM " +
                "TABLE(system_range(t._key, t._key + 1))) FROM person t").check(), IllegalArgumentException.class);

        assertQuery("SELECT t._key, (SELECT x FROM TABLE(system_range(t._key, t._key))) FROM person t").check();

        // Check exception on reduce phase.
        String cacheName = "person";

        IgniteCache<Integer, Employer> person = client.cache(cacheName);

        person.clear();

        for (int gridIdx = 0; gridIdx < nodeCount(); gridIdx++)
            person.put(primaryKey(grid(gridIdx).cache(cacheName)), new Employer(gridIdx == 0 ? "Emp" : null, 0.0d));

        GridTestUtils.assertThrowsWithCause(() -> assertQuery("SELECT (SELECT name FROM person)").check(),
            IllegalArgumentException.class);

        assertQuery("SELECT (SELECT name FROM person WHERE name is not null)").returns("Emp").check();
    }

    /** */
    @Test
    public void testAnyValAggr() {
        createAndPopulateTable();

        List<List<?>> res = executeSql("select any_value(name) from person");

        assertEquals(1, res.size());

        Object val = res.get(0).get(0);

        assertTrue("Unexpected value: " + val, "Igor".equals(val) || "Roma".equals(val) || "Ilya".equals(val));

        // Test with grouping.
        res = executeSql("select any_value(name), salary from person group by salary order by salary");

        assertEquals(2, res.size());

        val = res.get(0).get(0);

        assertTrue("Unexpected value: " + val, "Igor".equals(val) || "Roma".equals(val));

        val = res.get(1).get(0);

        assertEquals("Ilya", val);
    }

    /** */
    @Test
    public void testColocatedAggregate() throws Exception {
        executeSql("CREATE TABLE t1(id INT, val0 VARCHAR, val1 VARCHAR, val2 VARCHAR, PRIMARY KEY(id, val1)) " +
            "WITH AFFINITY_KEY=val1");

        executeSql("CREATE TABLE t2(id INT, val0 VARCHAR, val1 VARCHAR, val2 VARCHAR, PRIMARY KEY(id, val1)) " +
            "WITH AFFINITY_KEY=val1");

        for (int i = 0; i < 100; i++)
            executeSql("INSERT INTO t1 VALUES (?, ?, ?, ?)", i, "val" + i, "val" + i % 2, "val" + i);

        executeSql("INSERT INTO t2 VALUES (0, 'val0', 'val0', 'val0'), (1, 'val1', 'val1', 'val1')");

        String sql = "SELECT val1, count(val2) FROM t1 GROUP BY val1";

        assertQuery(sql)
            .matches(QueryChecker.matches(".*Exchange.*Colocated.*Aggregate.*"))
            .returns("val0", 50L)
            .returns("val1", 50L)
            .check();

        sql = "SELECT t2.val1, agg.cnt " +
            "FROM t2 JOIN (SELECT val1, COUNT(val2) AS cnt FROM t1 GROUP BY val1) AS agg ON t2.val1 = agg.val1";

        assertQuery(sql)
            .matches(QueryChecker.matches(".*Exchange.*Join.*Colocated.*Aggregate.*"))
            .returns("val0", 50L)
            .returns("val1", 50L)
            .check();
    }
}
