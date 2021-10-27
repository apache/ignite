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

import java.util.List;
import java.util.stream.Collectors;
import com.google.common.collect.ImmutableSet;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test SQL data types.
 */
public class DataTypesTest extends GridCommonAbstractTest {
    /** */
    private static QueryEngine qryEngine;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        IgniteEx grid = startGrid();

        qryEngine = Commons.lookupComponent(grid.context(), QueryEngine.class);
    }

    /**
     * Tests numeric types mapping on Java types.
     */
    @Test
    public void testNumericTypes() {
        try {
            executeSql("CREATE TABLE tbl(tiny TINYINT, small SMALLINT, i INTEGER, big BIGINT)");

            executeSql("INSERT INTO tbl VALUES (1, 2, 3, 4)");

            assertSingleEquals(executeSql("SELECT tiny FROM tbl"), (byte)1);
            assertSingleEquals(executeSql("SELECT small FROM tbl"), (short)2);
            assertSingleEquals(executeSql("SELECT i FROM tbl"), 3);
            assertSingleEquals(executeSql("SELECT big FROM tbl"), 4L);
        }
        finally {
            executeSql("DROP TABLE if exists tbl");
        }
    }

    /**
     * Tests numeric type convertation on equals.
     */
    @Test
    public void testNumericConvertationOnEquals() {
        try {
            executeSql("CREATE TABLE tbl(tiny TINYINT, small SMALLINT, i INTEGER, big BIGINT)");

            executeSql("INSERT INTO tbl VALUES (1, 2, 3, 4), (5, 5, 5, 5)");

            assertSingleEquals(executeSql("SELECT t1.tiny FROM tbl t1 JOIN tbl t2 ON (t1.tiny=t2.small)"), (byte)5);
            assertSingleEquals(executeSql("SELECT t1.small FROM tbl t1 JOIN tbl t2 ON (t1.small=t2.tiny)"), (short)5);

            assertSingleEquals(executeSql("SELECT t1.tiny FROM tbl t1 JOIN tbl t2 ON (t1.tiny=t2.i)"), (byte)5);
            assertSingleEquals(executeSql("SELECT t1.i FROM tbl t1 JOIN tbl t2 ON (t1.i=t2.tiny)"), 5);

            assertSingleEquals(executeSql("SELECT t1.tiny FROM tbl t1 JOIN tbl t2 ON (t1.tiny=t2.big)"), (byte)5);
            assertSingleEquals(executeSql("SELECT t1.big FROM tbl t1 JOIN tbl t2 ON (t1.big=t2.tiny)"), 5L);

            assertSingleEquals(executeSql("SELECT t1.small FROM tbl t1 JOIN tbl t2 ON (t1.small=t2.i)"), (short)5);
            assertSingleEquals(executeSql("SELECT t1.i FROM tbl t1 JOIN tbl t2 ON (t1.i=t2.small)"), 5);

            assertSingleEquals(executeSql("SELECT t1.small FROM tbl t1 JOIN tbl t2 ON (t1.small=t2.big)"), (short)5);
            assertSingleEquals(executeSql("SELECT t1.big FROM tbl t1 JOIN tbl t2 ON (t1.big=t2.small)"), 5L);

            assertSingleEquals(executeSql("SELECT t1.i FROM tbl t1 JOIN tbl t2 ON (t1.i=t2.big)"), 5);
            assertSingleEquals(executeSql("SELECT t1.big FROM tbl t1 JOIN tbl t2 ON (t1.big=t2.i)"), 5L);
        }
        finally {
            executeSql("DROP TABLE if exists tbl");
        }
    }

    /** */
    @Test
    public void testUnicodeStrings() {
        grid().getOrCreateCache(new CacheConfiguration<Integer, String>()
            .setName("string_cache")
            .setSqlSchema("PUBLIC")
            .setQueryEntities(F.asList(new QueryEntity(Integer.class, String.class).setTableName("string_table")))
        );

        String[] values = new String[] {"Кирилл", "Müller", "我是谁", "ASCII"};

        int key = 0;

        // Insert as inlined values.
        for (String val : values)
            executeSql("INSERT INTO string_table (_key, _val) VALUES (" + key++ + ", '" + val + "')");

        List<List<?>> rows = executeSql("SELECT _val FROM string_table");

        assertEquals(ImmutableSet.copyOf(values), rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        executeSql("DELETE FROM string_table");

        // Insert as parameters.
        for (String val : values)
            executeSql("INSERT INTO string_table (_key, _val) VALUES (?, ?)", key++, val);

        rows = executeSql("SELECT _val FROM string_table");

        assertEquals(ImmutableSet.copyOf(values), rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        rows = executeSql("SELECT substring(_val, 1, 2) FROM string_table");

        assertEquals(ImmutableSet.of("Ки", "Mü", "我是", "AS"),
            rows.stream().map(r -> r.get(0)).collect(Collectors.toSet()));

        for (String val : values) {
            rows = executeSql("SELECT char_length(_val) FROM string_table WHERE _val = ?", val);

            assertEquals(1, rows.size());
            assertEquals(val.length(), rows.get(0).get(0));
        }
    }

    /** */
    private List<List<?>> executeSql(String sql, Object... params) {
        return qryEngine.query(null, "PUBLIC", sql, params).get(0).getAll();
    }

    /** Checks type and value of single record result. */
    static void assertSingleEquals(List<List<?>> result, Object val) {
        assertNotNull(result);
        assertEquals(1, result.size());
        assertEquals(1, result.get(0).size());
        assertEquals(result.get(0).get(0).getClass(), val.getClass());
        assertEquals(result.get(0).get(0), val);
    }
}
