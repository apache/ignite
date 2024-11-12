/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.integration;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.AbstractNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static java.util.Collections.singletonList;

/**
 * Limit / offset tests.
 */
public class LimitOffsetIntegrationTest extends AbstractBasicIntegrationTransactionalTest {
    /** */
    private static IgniteCache<Integer, String> cacheRepl;

    /** */
    private static IgniteCache<Integer, String> cachePart;

    /** {@inheritDoc} */
    @Override protected void init() throws Exception {
        super.init();

        cacheRepl = client.cache("TEST_REPL");
        cachePart = client.cache("TEST_PART");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // Override method to keep caches after tests.
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        QueryEntity eRepl = new QueryEntity()
            .setTableName("TEST_REPL")
            .setKeyType(Integer.class.getName())
            .setValueType(String.class.getName())
            .setKeyFieldName("id")
            .setValueFieldName("val")
            .addQueryField("id", Integer.class.getName(), null)
            .addQueryField("val", String.class.getName(), null);

        QueryEntity ePart = new QueryEntity()
            .setTableName("TEST_PART")
            .setKeyType(Integer.class.getName())
            .setValueType(String.class.getName())
            .setKeyFieldName("id")
            .setValueFieldName("val")
            .addQueryField("id", Integer.class.getName(), null)
            .addQueryField("val", String.class.getName(), null);

        return super.getConfiguration(igniteInstanceName)
            .setCacheConfiguration(
                cacheConfiguration()
                    .setName(eRepl.getTableName())
                    .setCacheMode(CacheMode.REPLICATED)
                    .setQueryEntities(singletonList(eRepl))
                    .setSqlSchema("PUBLIC"),
                cacheConfiguration()
                    .setName(ePart.getTableName())
                    .setCacheMode(CacheMode.PARTITIONED)
                    .setQueryEntities(singletonList(ePart))
                    .setSqlSchema("PUBLIC"));
    }

    /** */
    @Test
    public void testNestedLimitOffsetWithUnion() {
        sql("INSERT into TEST_REPL VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd')");

        assertQuery("(SELECT id FROM TEST_REPL WHERE id = 2) UNION ALL " +
            "SELECT id FROM (select id from (SELECT id FROM TEST_REPL OFFSET 2) order by id OFFSET 1)"
        ).returns(2).returns(4).check();
    }

    /** Tests correctness of fetch / offset params. */
    @Test
    public void testInvalidLimitOffset() {
        String bigInt = BigDecimal.valueOf(10000000000L).toString();

        assertThrows("SELECT * FROM TEST_REPL OFFSET " + bigInt + " ROWS",
            SqlValidatorException.class, "Illegal value of offset");

        assertThrows("SELECT * FROM TEST_REPL FETCH FIRST " + bigInt + " ROWS ONLY",
            SqlValidatorException.class, "Illegal value of fetch / limit");

        assertThrows("SELECT * FROM TEST_REPL LIMIT " + bigInt,
            SqlValidatorException.class, "Illegal value of fetch / limit");

        assertThrows("SELECT * FROM TEST_REPL OFFSET -1 ROWS FETCH FIRST -1 ROWS ONLY",
            IgniteSQLException.class, null);

        assertThrows("SELECT * FROM TEST_REPL OFFSET -1 ROWS",
            IgniteSQLException.class, null);

        assertThrows("SELECT * FROM TEST_REPL OFFSET 2+1 ROWS",
            IgniteSQLException.class, null);

        // Check with parameters
        assertThrows("SELECT * FROM TEST_REPL OFFSET ? ROWS FETCH FIRST ? ROWS ONLY",
            SqlValidatorException.class, "Illegal value of fetch / limit", -1, -1);

        assertThrows("SELECT * FROM TEST_REPL OFFSET ? ROWS",
            SqlValidatorException.class, "Illegal value of offset", -1);

        assertThrows("SELECT * FROM TEST_REPL FETCH FIRST ? ROWS ONLY",
            SqlValidatorException.class, "Illegal value of fetch / limit", -1);
    }

    /**
     *
     */
    @Test
    public void testLimitOffset() throws Exception {
        int inBufSize = U.field(AbstractNode.class, "IN_BUFFER_SIZE");

        int[] rowsArr = {10, inBufSize, (2 * inBufSize) - 1};

        for (int rows : rowsArr) {
            fillCache(cacheRepl, rows);

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

    /** */
    @Test
    public void testLimitDistributed() throws Exception {
        fillCache(cachePart, 10_000);

        for (String order : F.asArray("id", "val")) { // Order by ID - without explicit IgniteSort node.
            List<List<?>> res = sql("SELECT id FROM TEST_PART ORDER BY " + order + " LIMIT 1000 OFFSET 5000");

            assertEquals(1000, res.size());

            for (int i = 0; i < 1000; i++)
                assertEquals(i + 5000, res.get(i).get(0));
        }
    }

    /** */
    @Test
    public void testOffsetOutOfRange() throws Exception {
        fillCache(cachePart, 5);

        assertQuery("SELECT (SELECT id FROM TEST_PART ORDER BY id LIMIT 1 OFFSET 10)").returns(NULL_RESULT).check();
    }

    /**
     * @param c Cache.
     * @param rows Rows count.
     */
    private void fillCache(IgniteCache<Integer, String> c, int rows) throws InterruptedException {
        clearTransaction();

        c.clear();

        for (int i = 0; i < rows; ++i)
            put(client, c, i, "val_" + String.format("%05d", i));

        awaitPartitionMapExchange();
    }

    /**
     * Check query with specified limit and offset (or without its when the arguments are negative),
     *
     * @param rows Rows count.
     * @param lim Limit.
     * @param off Offset.
     * @param param If {@code false} place limit/offset as literals. Otherwise they are plase as parameters.
     * @param sorted Use sorted query (adds ORDER BY).
     */
    void checkQuery(int rows, int lim, int off, boolean param, boolean sorted) {
        String sql = createSql(lim, off, param, sorted);

        Object[] params;
        if (lim >= 0 && off >= 0)
            params = new Object[]{off, lim};
        else if (lim >= 0)
            params = new Object[]{lim};
        else if (off >= 0)
            params = new Object[]{off};
        else
            params = X.EMPTY_OBJECT_ARRAY;

        log.info("SQL: " + sql + (param ? "params=" + Arrays.toString(params) : ""));

        List<List<?>> res = sql(sql, params);

        assertEquals("Invalid results size. [rows=" + rows + ", limit=" + lim + ", off=" + off
            + ", res=" + res.size() + ']', expectedSize(rows, lim, off), res.size());
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
            return lim;
        else if (off > rows)
            return 0;
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
        StringBuilder sb = new StringBuilder("SELECT * FROM TEST_REPL ");

        if (sorted)
            sb.append("ORDER BY ID ");

        if (off >= 0)
            sb.append("OFFSET ").append(param ? "?" : Integer.toString(off)).append(" ROWS ");

        if (lim >= 0)
            sb.append("FETCH FIRST ").append(param ? "?" : Integer.toString(lim)).append(" ROWS ONLY ");

        return sb.toString();
    }
}
