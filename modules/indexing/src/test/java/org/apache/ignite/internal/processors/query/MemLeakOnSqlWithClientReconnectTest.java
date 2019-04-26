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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.FieldsQueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests for group reservation leaks at the PartitionReservationManager on unstable topology.
 */
public class MemLeakOnSqlWithClientReconnectTest extends GridCommonAbstractTest {
    /** Keys count. */
    private static final int KEY_CNT = 10;

    /** Keys count. */
    private static final int ITERS = 2000;

    /** Replicated cache schema name. */
    private static final String REPL_SCHEMA = "REPL";

    /** Partitioned cache schema name. */
    private static final String PART_SCHEMA = "PART";

    /** Client node. */
    private IgniteEx cli;

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

        System.setProperty(IgniteSystemProperties.IGNITE_GROUP_RESERVATIONS_CACHE_MAX_SIZE, "4");

        startGrid();

        cli = (IgniteEx)startGrid("cli-main");

        IgniteCache<Long, Long> partCache = cli.createCache(new CacheConfiguration<Long, Long>()
            .setName("PART")
            .setSqlSchema("PART")
            .setQueryEntities(Collections.singleton(new QueryEntity(Long.class, Long.class)
                .setTableName("test")
                .addQueryField("id", Long.class.getName(), null)
                .addQueryField("val", Long.class.getName(), null)
                .setKeyFieldName("id")
                .setValueFieldName("val")
            ))
            .setAffinity(new RendezvousAffinityFunction(false, 10)));

        IgniteCache<Long, Long> replCache = cli.createCache(new CacheConfiguration<Long, Long>()
            .setName("REPL")
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

        System.clearProperty(IgniteSystemProperties.IGNITE_GROUP_RESERVATIONS_CACHE_MAX_SIZE);

        super.afterTest();
    }

    /**
     * Test partition group reservation leaks on partitioned cache.
     *
     * @throws Exception On error.
     */
    public void testPartitioned() throws Exception {
        checkReservationLeak(false);
    }

    /**
     * Test partition group reservation leaks on replicated cache.
     *
     * @throws Exception On error.
     */
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
            // Warm up.
            runQuery(cli, ITERS, replicated);

            int baseReservations = reservationCount(grid());

            // Run multiple queries on unstable topology.
            runQuery(cli, ITERS * 10, replicated);

            int curReservations = reservationCount(grid());

            assertTrue("Reservations leaks: [base=" + baseReservations + ", cur=" + curReservations + ']',
                curReservations < 10);

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
     * @param schema Schema name.
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

        Map reservations = GridTestUtils.getFieldValue(idx.mapQueryExecutor(), "reservations");

        return reservations.size();
    }
}
