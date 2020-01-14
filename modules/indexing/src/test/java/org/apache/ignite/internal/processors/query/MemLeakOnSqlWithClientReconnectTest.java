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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.cache.index.AbstractIndexingCommonTest;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

/**
 * Tests for group reservation leaks at the PartitionReservationManager on unstable topology.
 */
public class MemLeakOnSqlWithClientReconnectTest extends AbstractIndexingCommonTest {
    /** Keys count. */
    private static final int KEY_CNT = 10;

    /** Keys count. */
    private static final int ITERS = 2000;

    /** Replicated cache schema name. */
    private static final String REPL_SCHEMA = "REPL";

    /** Partitioned cache schema name. */
    private static final String PART_SCHEMA = "PART";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.startsWith("cli"))
            cfg.setClientMode(true).setGridLogger(new NullLogger());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        startGrid();

        IgniteCache<Long, Long> partCache = grid().createCache(new CacheConfiguration<Long, Long>()
            .setName("part")
            .setSqlSchema("PART")
            .setQueryEntities(Collections.singleton(new QueryEntity(Long.class, Long.class)
                .setTableName("test")
                .addQueryField("id", Long.class.getName(), null)
                .addQueryField("val", Long.class.getName(), null)
                .setKeyFieldName("id")
                .setValueFieldName("val")
            ))
            .setAffinity(new RendezvousAffinityFunction(false, 10)));

        IgniteCache<Long, Long> replCache = grid().createCache(new CacheConfiguration<Long, Long>()
            .setName("repl")
            .setSqlSchema("REPL")
            .setQueryEntities(Collections.singleton(new QueryEntity(Long.class, Long.class)
                .setTableName("test")
                .addQueryField("id", Long.class.getName(), null)
                .addQueryField("val", Long.class.getName(), null)
                .setKeyFieldName("id")
                .setValueFieldName("val")))
            .setCacheMode(CacheMode.REPLICATED));

        for (long i = 0; i < KEY_CNT; ++i) {
            partCache.put(i, i);
            replCache.put(i, i);
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Test partition group reservation leaks on partitioned cache.
     *
     * @throws Exception On error.
     */
    @Test
    public void testPartitioned() throws Exception {
        checkReservationLeak(false);
    }

    /**
     * Test partition group reservation leaks on replicated cache.
     *
     * @throws Exception On error.
     */
    @Test
    public void testReplicated() throws Exception {
        checkReservationLeak(true);
    }

    /**
     * Check partition group reservation leaks.
     *
     * @param replicated Flag to run query on partitioned or replicated cache.
     * @throws Exception On error.
     */
    private void checkReservationLeak(boolean replicated) throws Exception {
        final AtomicInteger cliNum = new AtomicInteger();
        final AtomicBoolean end = new AtomicBoolean();

        IgniteInternalFuture<Long> fut = GridTestUtils.runMultiThreadedAsync(() -> {
                String name = "cli_" + cliNum.getAndIncrement();

                while (!end.get()) {
                    try {
                        startGrid(name);

                        U.sleep(10);

                        stopGrid(name);
                    }
                    catch (Exception e) {
                        fail("Unexpected exception on start test client node");
                    }
                }
            },
            10, "cli-restart");

        try {

            String mainCliName = "cli-main";

            IgniteEx cli = startGrid(mainCliName);

            // Warm up.
            runQuery(cli, ITERS, replicated);

            int baseReservations = reservationCount(grid());

            // Run multiple queries on unstable topology.
            runQuery(cli, ITERS * 10, replicated);

            int curReservations = reservationCount(grid());

            assertTrue("Reservations leaks: [base=" + baseReservations + ", cur=" + curReservations + ']',
                curReservations < baseReservations * 2);

            log.info("Reservations OK: [base=" + baseReservations + ", cur=" + curReservations + ']');
        }
        finally {
            end.set(true);
        }

        fut.get();
    }

    /**
     * @param ign Ignite.
     * @param iters Run query 'iters' times
     * @param repl Run on replicated or partitioned cache.
     */
    private void runQuery(IgniteEx ign, int iters, boolean repl) {
        for (int i = 0; i < iters; ++i)
            sql(ign, repl ? REPL_SCHEMA : PART_SCHEMA,"SELECT * FROM test").getAll();
    }

    /**
     * @param ign Ignite instance.
     * @param sql SQL query.
     * @param args Query parameters.
     * @return Results cursor.
     */
    private FieldsQueryCursor<List<?>> sql(IgniteEx ign, String schema, String sql, Object... args) {
        return ign.context().query().querySqlFields(new SqlFieldsQuery(sql)
            .setSchema(schema)
            .setArgs(args), false);
    }

    /**
     * @param ign Ignite instance.
     * @return Count of reservations.
     */
    private static int reservationCount(IgniteEx ign) {
        IgniteH2Indexing idx = (IgniteH2Indexing)ign.context().query().getIndexing();

        Map reservations = GridTestUtils.getFieldValue(idx.partitionReservationManager(), "reservations");

        return reservations.size();
    }
}
