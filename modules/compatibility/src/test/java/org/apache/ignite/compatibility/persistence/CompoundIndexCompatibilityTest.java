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
 *
 */

package org.apache.ignite.compatibility.persistence;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compatibility.testframework.junits.SkipTestIfIsJdkNewer;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cache.query.index.IndexName;
import org.apache.ignite.internal.cache.query.index.sorted.inline.InlineIndexImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteInClosure;
import org.junit.Test;

/**
 * Tests compatibility for compound indexes.
 */
public class CompoundIndexCompatibilityTest extends IndexAbstractCompatibilityTest {
    /** */
    @Test
    @SkipTestIfIsJdkNewer(11)
    public void testSecondaryIndexesMigration_2_7_6() throws Exception {
        // 2.7.6 - version before _KEY unwrapping.
        doTestStartupWithOldVersion("2.7.6", () -> {
            checkIndex("_key_PK", null, "_KEY", "NAME");
            checkIndex("AFFINITY_KEY", "name='name1'", "NAME", "_KEY");
            checkIndex("IDX_CITY_AGE", "city='city1'", "CITY", "AGE", "_KEY", "NAME");
            checkIndex("IDX_AGE_NAME", "age=1", "AGE", "NAME", "_KEY");
            checkIndex("IDX_SALARY", "salary=0.1", "SALARY", "_KEY", "NAME");
            checkIndex("IDX_COMPANY", "company='company1'", "COMPANY", "_KEY", "NAME");
        });
    }

    /** */
    private void doTestStartupWithOldVersion(String ver, Runnable idxChecker) throws Exception {
        try {
            startGrid(1, ver, new ConfigurationClosure(true),
                new PostStartupClosure());

            stopAllGrids();

            IgniteEx igniteEx = startGrid(0);

            igniteEx.cluster().state(ClusterState.ACTIVE);

            fillData(igniteEx, 100, 200);

            idxChecker.run();

            igniteEx.cluster().state(ClusterState.INACTIVE);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private static class PostStartupClosure implements IgniteInClosure<Ignite> {
        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            ignite.active(true);

            IgniteEx igniteEx = (IgniteEx)ignite;

            initializeTables(igniteEx);

            ignite.active(false);
        }
    }

    /** */
    private static void initializeTables(IgniteEx igniteEx) {
        executeSql(igniteEx, "CREATE TABLE TEST (" +
            "id int, " +
            "name varchar, " +
            "age int, " +
            "city varchar(8), " +
            "salary decimal(10,2), " +
            "primary key (id, name, city)) WITH \"affinity_key=name,cache_name=cache\"");

        // Check different inline sizes (variable length columns, fixed length columns, not inlined types).
        executeSql(igniteEx, "CREATE INDEX IDX_CITY_AGE ON TEST (city, age)");
        executeSql(igniteEx, "CREATE INDEX IDX_AGE_NAME ON TEST (age, name)");
        executeSql(igniteEx, "CREATE INDEX IDX_SALARY ON TEST (salary)");

        // In versions before 2.14 fixed length columns added by "alter table" column inlined as variable length columns.
        executeSql(igniteEx, "ALTER TABLE TEST ADD company varchar(10)");
        executeSql(igniteEx, "CREATE INDEX IDX_COMPANY ON TEST (COMPANY)");

        fillData(igniteEx, 0, 100);
    }

    /** */
    private static void fillData(IgniteEx igniteEx, int from, int to) {
        for (int i = from; i < to; i++) {
            executeSql(igniteEx, "INSERT INTO TEST (id, name, age, company, city, salary) VALUES (?, ?, ?, ?, ?, ?)",
                i, "name" + i, i, "company" + i, "city" + i, BigDecimal.valueOf(i, 1));
        }
    }

    /**
     * @param name Index name.
     * @param cond Condition to select first row of the table by this index (if null - don't check query on index).
     * @param cols Index columns.
     */
    private void checkIndex(String name, String cond, String... cols) {
        // Check index columns.
        InlineIndexImpl idx = grid(0).context().indexProcessor().index(new IndexName("cache",
            "PUBLIC", "TEST", name)).unwrap(InlineIndexImpl.class);

        assertEquals(F.asList(cols), new ArrayList<>(idx.indexDefinition().indexKeyDefinitions().keySet()));

        // Check index is used by condition.
        if (cond == null)
            return;

        String sql = "SELECT id FROM TEST WHERE " + cond;

        List<List<?>> plan = executeSql(grid(0), "explain " + sql);

        assertFalse(plan.isEmpty());
        assertTrue("Actual plan: " + plan.get(0).get(0).toString() + " expected index: " + name,
            plan.get(0).get(0).toString().toLowerCase().contains(name.toLowerCase()));

        // Check query on index.
        assertScalarResult(grid(0), sql, 1);
    }

    /** */
    private static void assertScalarResult(IgniteEx igniteEx, String sql, Object expRes, Object... args) {
        List<List<?>> results = executeSql(igniteEx, sql, args);

        assertEquals(1, results.size());
        assertEquals(expRes, results.get(0).get(0));
    }
}
