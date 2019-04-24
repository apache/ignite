/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Test for cancel of query containing distributed joins.
 */
public class CancelLazyDistributedJoinQueryTest extends AbstractIndexingCommonTest {
    /** Nodes. */
    private static final int NODES = 3;

    /** Rows. */
    private static final int ROWS = 10_000;

    /** Iterations. */
    private static final int ITERS = 100;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODES);

        populateData();
    }

    /**
     *
     */
    private void populateData() {
        sql("CREATE TABLE TBL0 (id INT PRIMARY KEY, name VARCHAR)");
        sql("CREATE TABLE TBL1 (id INT PRIMARY KEY, id0 INT, name VARCHAR)");
        sql("CREATE TABLE TBL2 (id INT PRIMARY KEY, id1 INT, name VARCHAR)");

        sql("CREATE INDEX idx1_0 ON TBL1 (id0)");
        sql("CREATE INDEX idx2_1 ON TBL2 (id1)");

        for (int i = 0; i < ROWS; ++i) {
            sql("INSERT INTO TBL0 (id, name) VALUES (?, ?)", i, "val0_" + i);
            sql("INSERT INTO TBL1 (id, id0, name) VALUES (?, ?, ?)", i + 1, i, "val1_" + i);
            sql("INSERT INTO TBL2 (id, id1, name) VALUES (?, ?, ?)", i + 2, i, "val1_" + i);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     * @throws Exception On failed.
     */
    @Test
    public void testQueryCancel() throws Exception {
        for (int i = 0; i < ITERS; ++i)
            checkQueryCancel(i % 2 == 0);
    }

    /**
     * @param cancelByCursor Cancels query by cursor.close() or uses GridQueryCancel.cancel().
     * @throws Exception On failed.
     */
    private void checkQueryCancel(final boolean cancelByCursor) throws Exception {
        final GridQueryCancel cancel = new GridQueryCancel();

        List<FieldsQueryCursor<List<?>>> curs = grid(0).context().query().querySqlFields(
            null,
            new SqlFieldsQuery(
                "SELECT t0.name, t1.name, t2.name " +
                    "FROM TBL0 as t0 " +
                    "LEFT JOIN TBL1 as t1 ON t0.id = t1.id0 " +
                    "LEFT JOIN TBL2 as t2 ON t1.id = t2.id1")
                .setLazy(true)
                .setDistributedJoins(true),
            null,
            false,
            true,
            cancel);

        assertEquals(1, curs.size());

        final FieldsQueryCursor<List<?>> cur = curs.get(0);

        Iterator<List<?>> it = cur.iterator();

        final AtomicInteger cnt = new AtomicInteger();

        IgniteInternalFuture fut = GridTestUtils.runAsync(() -> {
            try {
                while (cnt.get() < 2000)
                    U.sleep(1);

                if (cancelByCursor)
                    cur.close();
                else
                    cancel.cancel();
            }
            catch (IgniteInterruptedCheckedException e) {
                log.warning("IgniteInterruptedCheckedException", e);
            }
        });

        try {
            for (; it.hasNext(); cnt.incrementAndGet())
                it.next();

            fail("The query must be canceled");
        }
        catch (Exception e) {
            log.warning("Exception", e);
        }

        fut.get();
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return grid(0).context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setLazy(true)
            .setDistributedJoins(true)
            .setArgs(args), false);
    }
}
