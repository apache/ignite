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

package org.apache.ignite.internal.calcite;

import java.util.List;

import org.apache.ignite.lang.IgniteException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-15655")
public class ITAggregatesTest extends AbstractBasicIntegrationTest {
    /** */
    @BeforeAll
    static void initTestData() {
        createAndPopulateTable();
    }

    /** */
    @Test
    public void countOfNonNumericField() {
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

        assertQuery("select count(name), name from person group by name")
            .returns(1L, "Igor")
            .returns(1L, "Ilya")
            .returns(2L, "Roma")
            .returns(0L, null)
            .check();

        assertQuery("select avg(salary) from person")
            .returns(12.0)
            .check();

        assertQuery("select name, salary from person where person.salary > (select avg(person.salary) from person)")
            .returns(null, 15d)
            .returns("Ilya", 15d)
            .check();

        assertQuery("select avg(salary) from (select avg(salary) as salary from person union all select salary from person)")
            .returns(12d)
            .check();
    }

    /** */
    @Test
    public void testMultipleRowsFromSingleAggr() {
        assertThrows(
            IgniteException.class,
            () -> assertQuery("SELECT (SELECT name FROM person)").check()
        );

        assertThrows(
            IgniteException.class,
            () -> assertQuery("SELECT t.id, (SELECT x FROM TABLE(system_range(1, 5))) FROM person t").check()
        );

        assertThrows(
            IgniteException.class,
            () -> assertQuery("SELECT t.id, (SELECT x FROM " +
                "TABLE(system_range(t.id, t.id + 1))) FROM person t").check()
        );

        assertQuery("SELECT t.id, (SELECT x FROM TABLE(system_range(t.id, t.id))) FROM person t").check();
    }

    /** */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-14597")
    @Test
    public void testAnyValAggr() {
        List<List<?>> res = sql("select any_value(name) from person");

        assertEquals(1, res.size());

        Object val = res.get(0).get(0);

        assertTrue("Igor".equals(val) || "Roma".equals(val) || "Ilya".equals(val), "Unexpected value: " + val);

        // Test with grouping.
        res = sql("select any_value(name), salary from person group by salary order by salary");

        assertEquals(2, res.size());

        val = res.get(0).get(0);

        assertTrue("Igor".equals(val) || "Roma".equals(val), "Unexpected value: " + val);

        val = res.get(1).get(0);

        assertEquals("Ilya", val);
    }
}
