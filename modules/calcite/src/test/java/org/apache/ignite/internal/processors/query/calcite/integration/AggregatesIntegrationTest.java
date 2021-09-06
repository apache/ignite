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
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 *
 */
public class AggregatesIntegrationTest extends AbstractBasicIntegrationTest {
    /** */
    @Test
    public void countOfNonNumericField() {
        createAndPopulateTable();

        assertQuery("select count(name) from person").returns(4L).check();
        assertQuery("select count(*) from person").returns(5L).check();
        assertQuery("select count(1) from person").returns(5L).check();
        assertQuery("select count(null) from person").returns(0L).check();

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
            .returns(10d, 3L, 3)
            .returns(15d, 2L, 2)
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

        List<List<?>> res = execute("select any_value(name) from person");

        assertEquals(1, res.size());

        Object val = res.get(0).get(0);

        assertTrue("Unexpected value: " + val, "Igor".equals(val) || "Roma".equals(val) || "Ilya".equals(val));

        // Test with grouping.
        res = execute("select any_value(name), salary from person group by salary order by salary");

        assertEquals(2, res.size());

        val = res.get(0).get(0);

        assertTrue("Unexpected value: " + val, "Igor".equals(val) || "Roma".equals(val));

        val = res.get(1).get(0);

        assertEquals("Ilya", val);
    }

    /** */
    private List<List<?>> execute(String sql) {
        List<FieldsQueryCursor<List<?>>> cursors = Commons.lookupComponent(client.context(), QueryEngine.class)
            .query(null, "PUBLIC", sql);

        return cursors.get(0).getAll();
    }
}
