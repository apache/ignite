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
import java.util.List;
import org.apache.calcite.linq4j.function.BigDecimalFunction1;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.QueryEngine;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Collections.singletonList;

/**
 * Limit / offset tests.
 */
public class LimitOffsetTest extends GridCommonAbstractTest {
    /** */
    private static IgniteCache<Integer, String> cache;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);

        QueryEntity e = new QueryEntity()
            .setTableName("Test")
            .setKeyType(Integer.class.getName())
            .setValueType(String.class.getName())
            .setKeyFieldName("id")
            .setValueFieldName("val")
            .addQueryField("id", Integer.class.getName(), null)
            .addQueryField("val", String.class.getName(), null);

        CacheConfiguration<Integer, String> ccfg = cacheConfiguration(e);

        cache = grid(0).createCache(ccfg);
    }

    /** */
    private CacheConfiguration cacheConfiguration(QueryEntity ent) {
        return new CacheConfiguration<>(ent.getTableName())
            .setCacheMode(CacheMode.REPLICATED)
            .setBackups(1)
            .setQueryEntities(singletonList(ent))
            .setSqlSchema("PUBLIC");
    }

    /**
     *
     */
    @Test
    public void testInvalidLimitOffset() {
        QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);

        String bigInt = BigDecimal.valueOf(10000000000L).toString();
        {
            List<FieldsQueryCursor<List<?>>> cursors =
                engine.query(null, "PUBLIC",
                    "SELECT * FROM TEST OFFSET " + bigInt + " ROWS FETCH FIRST " + bigInt + " ROWS ONLY",
                    X.EMPTY_OBJECT_ARRAY);
            cursors.get(0).getAll();
        }
        GridTestUtils.assertThrows(log, () -> {
            List<FieldsQueryCursor<List<?>>> cursors =
                engine.query(null, "PUBLIC",
                    "SELECT * FROM TEST OFFSET -1 ROWS FETCH FIRST -1 ROWS ONLY",
                    X.EMPTY_OBJECT_ARRAY);
            cursors.get(0).getAll();

            return null;
        }, IgniteSQLException.class, "Failed to parse query");

        GridTestUtils.assertThrows(log, () -> {
            List<FieldsQueryCursor<List<?>>> cursors =
                engine.query(null, "PUBLIC",
                    "SELECT * FROM TEST OFFSET -1 ROWS",
                    X.EMPTY_OBJECT_ARRAY);
            cursors.get(0).getAll();

            return null;
        }, IgniteSQLException.class, "Failed to parse query");

        GridTestUtils.assertThrows(log, () -> {
            List<FieldsQueryCursor<List<?>>> cursors =
                engine.query(null, "PUBLIC",
                    "SELECT * FROM TEST FETCH FIRST -1 ROWS ONLY",
                    X.EMPTY_OBJECT_ARRAY);
            cursors.get(0).getAll();

            return null;
        }, IgniteSQLException.class, "Failed to parse query");

        // Check with parameters
        GridTestUtils.assertThrows(log, () -> {
            List<FieldsQueryCursor<List<?>>> cursors =
                engine.query(null, "PUBLIC",
                    "SELECT * FROM TEST OFFSET ? ROWS FETCH FIRST ? ROWS ONLY",
                    -1, -1);
            cursors.get(0).getAll();

            return null;
        }, IgniteSQLException.class, "Invalid query offset: -1");

        GridTestUtils.assertThrows(log, () -> {
            List<FieldsQueryCursor<List<?>>> cursors =
                engine.query(null, "PUBLIC",
                    "SELECT * FROM TEST OFFSET ? ROWS",
                    -1);
            cursors.get(0).getAll();

            return null;
        }, IgniteSQLException.class, "Invalid query offset: -1");

        GridTestUtils.assertThrows(log, () -> {
            List<FieldsQueryCursor<List<?>>> cursors =
                engine.query(null, "PUBLIC",
                    "SELECT * FROM TEST FETCH FIRST ? ROWS ONLY",
                    -1);
            cursors.get(0).getAll();

            return null;
        }, IgniteSQLException.class, "Invalid query limit: -1");
    }

    /**
     *
     */
    @Test
    public void testDbg() {
        fillCache(512);

        QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);

        for (int i = 0; i < 100; ++i) {
            log.info("+++ ITER " + i);
            List<FieldsQueryCursor<List<?>>> cursors =
                engine.query(null, "PUBLIC",
                    "SELECT * FROM TEST FETCH FIRST 512 ROWS ONLY",
                    0);

            cursors.get(0).getAll();
        }

    }

    /**
     *
     */
    @Test
    public void testLimitOffset() {
        int[] rowsArr = {10, 512, 2000};

        for (int rows : rowsArr) {
            fillCache(rows);

            int[] limits = {-1, 0, 10, rows / 2 - 1, rows / 2, rows / 2 + 1, rows - 1, rows};
            int[] offsets = {-1, 0, 10, rows / 2 - 1, rows / 2, rows / 2 + 1, rows - 1, rows};

            for (int lim : limits) {
                for (int off : offsets) {
                    log.info("+++ Check [rows=" + rows + ", limit=" + lim + ", off=" + off + ']');

                    checkQuery(rows, lim, off, false, false);
                    checkQuery(rows, lim, off, true, false);
                    checkQuery(rows, lim, off, false, true);
                    checkQuery(rows, lim, off, true, true);
                }
            }
        }
    }

    /**
     * @param rows
     */
    private void fillCache(int rows) {
        cache.clear();

        for (int i = 0; i < rows; ++i)
            cache.put(i, "val_" + i);
    }

    /**
     * Check query with specified limit and offset (or without its when the arguments are negative),
     *
     * @param rows
     * @param lim Limit.
     * @param off Offset.
     * @param param If {@code false} place limit/offset as literals. Otherwise they are plase as parameters.
     * @param sorted Use sorted query (adds ORDER BY).
     */
    void checkQuery(int rows, int lim, int off, boolean param, boolean sorted) {
        QueryEngine engine = Commons.lookupComponent(grid(0).context(), QueryEngine.class);

        String sql = createSql(lim, off, param, sorted);

        log.info("SQL: " + sql);

        Object [] params;
        if (lim >= 0 && off >= 0)
            params = new Object[]{off, lim};
        else if (lim >= 0)
            params = new Object[]{lim};
        else if (off >= 0)
            params = new Object[]{off};
        else
            params = X.EMPTY_OBJECT_ARRAY;

        List<FieldsQueryCursor<List<?>>> cursors =
            engine.query(null, "PUBLIC", sql, param ? params : X.EMPTY_OBJECT_ARRAY);

        List<List<?>> res = cursors.get(0).getAll();

        assertEquals("Invalid results size. [rows=" + rows +", limit=" + lim + ", off=" + off + ", res=" + res.size() + ']',
            expectedSize(rows, lim, off), res.size());
    }

    /**
     * Calculates expected result set size by limit and offset.
     */
    private int expectedSize(int rows, int lim, int off) {
        if (off < 0)
            off = 0;

        if (lim == 0)
            return 0;
        else if (lim < 0)
            return rows - off;
        else if (lim + off < rows)
            return  lim;
        else if (off > rows)
            return  0;
        else
            return rows - off;
    }

    /**
     * @param lim Limit.
     * @param off Offset.
     * @param param Flag to place limit/offset  by parameter or literal.
     * @return SQL query string.
     */
    private String createSql(int lim, int off, boolean param, boolean sorted) {
        StringBuilder sb = new StringBuilder("SELECT * FROM TEST ");

        if (sorted)
            sb.append("ORDER BY ID ");

        if (off >= 0)
            sb.append("OFFSET ").append(param ? "?" : Integer.toString(off)).append(" ROWS ");

        if (lim >= 0)
            sb.append("FETCH FIRST ").append(param ? "?" : Integer.toString(lim)).append(" ROWS ONLY");

        return sb.toString();
    }
}
