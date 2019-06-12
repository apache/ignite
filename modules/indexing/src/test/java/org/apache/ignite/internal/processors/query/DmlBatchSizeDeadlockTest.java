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

package org.apache.ignite.internal.processors.query;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests DML deadlock with different update batch size.
 */
public class DmlBatchSizeDeadlockTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEY_CNT = 1000;

    /** Test time to run. */
    private static final int TEST_TIME = 20_000;

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @throws IgniteCheckedException On error.
     */
    @Test
    public void testDeadlockOnDmlAtomic() throws IgniteCheckedException {
        checkDeadlockOnDml(CacheAtomicityMode.ATOMIC);
    }

    /**
     * @throws IgniteCheckedException On error.
     */
    @Test
    public void testDeadlockOnDmlTransactional() throws IgniteCheckedException {
        checkDeadlockOnDml(CacheAtomicityMode.TRANSACTIONAL);
    }

    /**
     * @param mode Atomicity mode.
     * @throws IgniteCheckedException On failed.
     */
    public void checkDeadlockOnDml(CacheAtomicityMode mode) throws IgniteCheckedException {
        IgniteCache<Long, Long> cache = createCache(mode);

        final long tEnd = U.currentTimeMillis() + TEST_TIME;

        final IgniteInternalFuture futAsc = GridTestUtils.runAsync(() -> {
            while (U.currentTimeMillis() < tEnd) {
                try {
                    sql("UPDATE test SET val = 2 ORDER BY id ASC");
                }
                catch (Exception e) {
                    IgniteSQLException esql = X.cause(e, IgniteSQLException.class);

                    if (esql == null || !esql.getMessage().contains("Failed to update some keys because they " +
                        "had been modified concurrently"))
                        throw e;
                }
            }
        });

        final IgniteInternalFuture futDesc = GridTestUtils.runAsync(() -> {
            while (U.currentTimeMillis() < tEnd) {
                while (U.currentTimeMillis() < tEnd) {
                    try {
                        sql("UPDATE test SET val = 3 ORDER BY id DESC");
                    }
                    catch (Exception e) {
                        IgniteSQLException esql = X.cause(e, IgniteSQLException.class);

                        if (esql == null || !esql.getMessage().contains("Failed to update some keys because they " +
                            "had been modified concurrently"))
                            throw e;
                    }
                }
            }
        });

        final IgniteInternalFuture futCache = GridTestUtils.runAsync(() -> {
            while (U.currentTimeMillis() < tEnd) {
                Map<Long, Long> map = new LinkedHashMap();

                for (long i = KEY_CNT - 1; i >= 0; --i)
                    map.put(i, i);

                cache.putAll(map);
            }
        });

        boolean deadlock = !GridTestUtils.waitForCondition(
            () -> futAsc.isDone() && futDesc.isDone() && futCache.isDone(),
            TEST_TIME + 5000);

        if (deadlock) {
            futAsc.cancel();
            futDesc.cancel();
            futCache.cancel();

            fail("Deadlock on DML");
        }
    }

    /**
     * @param mode Cache atomicity mode.
     * @return Created test cache.
     */
    private IgniteCache<Long, Long> createCache(CacheAtomicityMode mode) {
        IgniteCache<Long, Long> c = grid().createCache(new CacheConfiguration<Long, Long>()
            .setName("test")
            .setSqlSchema("TEST")
            .setAtomicityMode(mode)
            .setQueryEntities(Collections.singleton(new QueryEntity(Long.class, Long.class)
                .setTableName("test")
                .addQueryField("id", Long.class.getName(), null)
                .addQueryField("val", Long.class.getName(), null)
                .setKeyFieldName("id")
                .setValueFieldName("val")
            ))
            .setAffinity(new RendezvousAffinityFunction(false, 10)));

        for (long i = 0; i < KEY_CNT; ++i)
            c.put(i, i);

        return c;
    }

    /**
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(String sql, Object... args) {
        return grid().context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setSchema("TEST")
            .setUpdateBatchSize(1)
            .setArgs(args), false);
    }
}
