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
import org.apache.ignite.internal.processors.query.calcite.exec.rel.AbstractNode;
import org.apache.ignite.internal.processors.query.calcite.exec.rel.SortNode;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.junit.Test;

import static java.util.Collections.singletonList;

/**
 * Limit / offset tests.
 */
public class LimitOffsetIntegrationTest extends AbstractBasicIntegrationTransactionalTest {
    /** Log listener. */
    private static final ListeningTestLogger listeningLog = new ListeningTestLogger(log);

    /** */
    private static final String ILLEGAL_FETCH_VAL_ERR_MSG = "Illegal value of fetch / limit. " +
        "The value must be non-negative and less than or equal to 9223372036854775807";

    /** */
    private static final String ENCOUNTERED_ERR_MSG = "Encountered";

    /** */
    private static final String FAIL_PARSE_ERR_MSG = "Failed to parse query";

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
            .setGridLogger(listeningLog)
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
    public void testNestedLimitOffsetWithUnion() throws Exception {
        fillCache(cacheRepl, 4);

        assertQuery("(SELECT id FROM TEST_REPL WHERE id = 1) UNION ALL " +
            "SELECT id FROM (select id from (SELECT id FROM TEST_REPL OFFSET 2) order by id OFFSET 1)"
        ).returns(1).returns(3).check();
    }

    /** Tests correctness of fetch / offset params. */
    @Test
    public void testInvalidLimitOffset() {
        assertThrows("SELECT * FROM TEST_REPL OFFSET " + moreThanMaxLong() + " ROWS",
            SqlValidatorException.class, "Illegal value of offset");

        assertThrows("SELECT * FROM TEST_REPL FETCH FIRST " + moreThanMaxLong() + " ROWS ONLY",
            SqlValidatorException.class, ILLEGAL_FETCH_VAL_ERR_MSG);

        assertThrows("SELECT * FROM TEST_REPL LIMIT " + moreThanMaxLong(),
            SqlValidatorException.class, ILLEGAL_FETCH_VAL_ERR_MSG);

        assertThrowsSqlException("SELECT * FROM TEST_REPL OFFSET -1 ROWS FETCH FIRST -1 ROWS ONLY", FAIL_PARSE_ERR_MSG);

        assertThrowsSqlException("SELECT * FROM TEST_REPL OFFSET -1 ROWS", null);
        assertThrowsSqlException("SELECT * FROM TEST_REPL OFFSET -1.5 ROWS", null);
        assertThrowsSqlException("SELECT * FROM TEST_REPL OFFSET -0.5 ROWS", null);
        assertThrowsSqlException("SELECT * FROM TEST_REPL OFFSET 2+1 ROWS", null);
        assertThrowsSqlException("SELECT * FROM TEST_REPL OFFSET id ROWS", null);

        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST -1 ROWS ONLY", ENCOUNTERED_ERR_MSG);
        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST -1.5 ROWS ONLY", ENCOUNTERED_ERR_MSG);
        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST -0.5 ROWS ONLY", ENCOUNTERED_ERR_MSG);
        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST 2+1 ROWS ONLY", ENCOUNTERED_ERR_MSG);
        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST id ROWS ONLY", ENCOUNTERED_ERR_MSG);
    }

    /** */
    @Test
    public void testFractionalLimitOffset() throws Exception {
        fillCache(cacheRepl, 4);

        assertQuery("SELECT id FROM TEST_REPL ORDER BY id LIMIT 0.5").returns(0).check();
        assertQuery("SELECT id FROM TEST_REPL ORDER BY id LIMIT 1.2").returns(0).check();
        assertQuery("SELECT id FROM TEST_REPL ORDER BY id LIMIT 1.5").returns(0).returns(1).check();

        assertQuery("SELECT id FROM TEST_REPL ORDER BY id FETCH FIRST 0.5 ROWS ONLY").returns(0).check();
        assertQuery("SELECT id FROM TEST_REPL ORDER BY id FETCH FIRST 1.3 ROWS ONLY").returns(0).check();
        assertQuery("SELECT id FROM TEST_REPL ORDER BY id FETCH FIRST 1.6 ROWS ONLY").returns(0).returns(1).check();

        assertQuery("SELECT id FROM TEST_REPL ORDER BY id OFFSET 0.5 ROWS")
            .returns(1).returns(2).returns(3).check();
        assertQuery("SELECT id FROM TEST_REPL ORDER BY id OFFSET 2.3 ROWS").returns(2).returns(3).check();
        assertQuery("SELECT id FROM TEST_REPL ORDER BY id OFFSET 2.6 ROWS").returns(3).check();
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

    /** */
    @Test
    public void testInvalidFetchExpression() {
        BigDecimal moreThanMaxLong = moreThanMaxLong();

        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST (" + moreThanMaxLong + ") ROWS ONLY", ILLEGAL_FETCH_VAL_ERR_MSG);
        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST (" + moreThanMaxLong + " + 1) ROWS ONLY", ILLEGAL_FETCH_VAL_ERR_MSG);
        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST (1 + " + moreThanMaxLong + ") ROWS ONLY", ILLEGAL_FETCH_VAL_ERR_MSG);

        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST (-2) ROWS ONLY", ILLEGAL_FETCH_VAL_ERR_MSG);
        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST (2 - 3) ROWS ONLY", ILLEGAL_FETCH_VAL_ERR_MSG);
        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST (1 + 2 - 4) ROWS ONLY", ILLEGAL_FETCH_VAL_ERR_MSG);
        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST (-1.5) ROWS ONLY", ILLEGAL_FETCH_VAL_ERR_MSG);

        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST ('abc') ROWS ONLY", ILLEGAL_FETCH_VAL_ERR_MSG);
        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST (SUBSTRING('abc', 1, 1)) ROWS ONLY", ILLEGAL_FETCH_VAL_ERR_MSG);
        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST (CAST(NULL AS INTEGER)) ROWS ONLY", ILLEGAL_FETCH_VAL_ERR_MSG);
        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST (NULLIF(1, 1)) ROWS ONLY",
            ILLEGAL_FETCH_VAL_ERR_MSG);

        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST (id) ROWS ONLY", ILLEGAL_FETCH_VAL_ERR_MSG);
        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST (id + 1) ROWS ONLY", ILLEGAL_FETCH_VAL_ERR_MSG);
        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST (ABS(id)) ROWS ONLY", ILLEGAL_FETCH_VAL_ERR_MSG);

        assertThrowsSqlException("SELECT * FROM TEST_REPL FETCH FIRST SQRT(4) ROWS ONLY", ENCOUNTERED_ERR_MSG);
    }

    /** */
    @Test
    public void testFetchExpression() throws Exception {
        fillCache(cacheRepl, 5);

        assertQuery("SELECT id FROM TEST_REPL ORDER BY id FETCH FIRST (1) ROWS ONLY")
            .returns(0)
            .check();

        assertQuery("SELECT id FROM TEST_REPL ORDER BY id FETCH FIRST (1 + 5 - 2) ROWS ONLY")
            .returns(0)
            .returns(1)
            .returns(2)
            .returns(3)
            .check();

        assertQuery("SELECT id FROM TEST_REPL ORDER BY id FETCH FIRST (1 + (2 - 1) + 1) ROWS ONLY")
            .returns(0)
            .returns(1)
            .returns(2)
            .check();

        assertQuery("SELECT id FROM TEST_REPL ORDER BY id DESC FETCH FIRST (1 + (2 - 1) + 1) ROWS ONLY")
            .returns(4)
            .returns(3)
            .returns(2)
            .check();

        assertQuery("SELECT id FROM TEST_REPL ORDER BY id FETCH FIRST (ABS(-2)) ROWS ONLY")
            .returns(0)
            .returns(1)
            .check();

        assertQuery("SELECT id FROM TEST_REPL ORDER BY id FETCH FIRST (1 + ABS(-2)) ROWS ONLY")
            .returns(0)
            .returns(1)
            .returns(2)
            .check();

        assertQuery("SELECT id FROM TEST_REPL ORDER BY id FETCH FIRST (SQRT(4) + 1 + 0) ROWS ONLY")
            .returns(0)
            .returns(1)
            .returns(2)
            .check();

        assertQuery("SELECT id FROM TEST_REPL ORDER BY id FETCH FIRST (EXTRACT(YEAR FROM CURRENT_DATE)) ROWS ONLY")
            .returns(0)
            .returns(1)
            .returns(2)
            .returns(3)
            .returns(4)
            .check();
    }

    /** */
    @Test
    public void testZeroFetchExpressionWithLiteralDoesNotFailFragments() throws Exception {
        fillCache(cacheRepl, 5);

        LogListener sortAssertionError = LogListener.matches(msg ->
            msg.contains(AssertionError.class.getName()) && msg.contains(SortNode.class.getName())).build();

        listeningLog.registerListener(sortAssertionError);

        try {
            assertQuery("SELECT id FROM TEST_REPL ORDER BY id FETCH FIRST (ABS(0)) ROWS ONLY")
                .resultSize(0)
                .check();

            assertFalse("Unexpected AssertionError in SortNode", sortAssertionError.check(1_000L));
        }
        finally {
            listeningLog.unregisterListener(sortAssertionError);
        }
    }

    /** */
    @Test
    public void testFetchExpressionNested() throws Exception {
        fillCache(cacheRepl, 5);

        assertQuery("SELECT id FROM (SELECT id from TEST_REPL ORDER BY id FETCH FIRST (2 + 3) ROWS ONLY) " +
            "ORDER BY id FETCH NEXT (1 + 1) ROWS ONLY")
            .returns(0)
            .returns(1)
            .check();
    }

    /** */
    @Test
    public void testFetchExpressionWithNvlAndCoalesce() throws Exception {
        fillCache(cacheRepl, 5);

        assertQuery("SELECT id FROM TEST_REPL ORDER BY id FETCH FIRST (1 + NVL(1, 10000)) ROWS ONLY")
            .returns(0)
            .returns(1)
            .check();

        assertQuery("SELECT id FROM TEST_REPL ORDER BY id FETCH FIRST (COALESCE(2, 10000)) ROWS ONLY")
            .returns(0)
            .returns(1)
            .check();
    }

    /** */
    @Test
    public void testFetchExpressionWithoutPushDown() throws Exception {
        fillCache(cacheRepl, 5);

        assertQuery("SELECT id FROM TEST_REPL ORDER BY id OFFSET 1 ROWS "
                + "FETCH FIRST (ABS(0.5)) ROWS ONLY")
            .resultSize(0)
            .check();

        assertQuery("SELECT id FROM TEST_REPL ORDER BY id OFFSET 1 ROWS "
                + "FETCH FIRST (RAND_INTEGER(1) + 2) ROWS ONLY")
            .returns(1)
            .returns(2)
            .check();
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

        List<List<?>> res = sql(sql, param ? params : X.EMPTY_OBJECT_ARRAY);

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
     * @param param Flag to place limit/offset by parameter or literal.
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

    /** */
    private static BigDecimal moreThanMaxLong() {
        return BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);
    }
}
