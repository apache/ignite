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

package org.apache.ignite.internal.processors.cache.distributed.near;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.query.h2.IgniteH2Indexing;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests distributed fields query resources cleanup on node failure.
 * Grids must be restarted after each test.
 */
public class IgniteCacheQueryStopNodeFailSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String KEY_CROSS_JOIN_QRY = "select a._key, b._key from String a, String b";

    /** */
    public static final int GRID_COUNT = 3;

    /** */
    public static final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /**
     * Default constructor.
     */
    public IgniteCacheQueryStopNodeFailSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(GRID_COUNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        if ("client".equals(gridName))
            c.setClientMode(true);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        CacheConfiguration<?, ?> cc = defaultCacheConfiguration();

        cc.setBackups(0);
        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setAtomicityMode(TRANSACTIONAL);
        cc.setRebalanceMode(SYNC);
        cc.setAffinity(new RendezvousAffinityFunction(false, 60));
        cc.setIndexedTypes(Integer.class, String.class);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * Tests stopping two-step query on initiator node fail.
     */
    public void testRemoteQueryExecutionStopClientNodeFail1() throws Exception {
        testQueryStopOnNodeFail(10_000, 4, KEY_CROSS_JOIN_QRY, 500, 0, -1);
    }

    /**
     * Tests stopping two-step query on initiator node fail.
     */
    public void testRemoteQueryExecutionStopClientNodeFail2() throws Exception {
        testQueryStopOnNodeFail(10_000, 4, KEY_CROSS_JOIN_QRY, 3_000, 0, -1);
    }

    /**
     * Tests stopping two-step query on executor node fail.
     */
    public void testRemoteQueryExecutionStopServerNodeFail1() throws Exception {
        testQueryStopOnNodeFail(10_000, 4, KEY_CROSS_JOIN_QRY, 3_000, 0, 1);
    }

    /**
     * Tests stopping two-step query on executor node fail.
     */
    public void testRemoteQueryExecutionStopServerNodeFail2() throws Exception {
        testQueryStopOnNodeFail(10_000, 4, KEY_CROSS_JOIN_QRY, 3_000, 0, 2);
    }

    /**
     * Tests stopping two step query when client or server node fails.
     */
    private void testQueryStopOnNodeFail(int keyCnt, int valSize, String sql, final long failTimeout,
        final int kickGridIdx, final int failGridIdx) throws Exception {
        try (final Ignite client = startGrid("client")) {

            assertTrue(client.cluster().localNode().isClient());

            IgniteCache<Object, Object> cache = client.cache(null);

            assertEquals(0, cache.localSize());

            for (int i = 0; i < keyCnt; i++) {
                char[] tmp = new char[valSize];
                Arrays.fill(tmp, ' ');
                cache.put(i, new String(tmp));
            }

            assertEquals(0, cache.localSize());

            final QueryCursor<List<?>> qry = cache.query(new SqlFieldsQuery(sql));

            final CountDownLatch l = new CountDownLatch(1);

            client.scheduler().runLocal(new Runnable() {
                @Override public void run() {
                    try {
                        failNode(grid(kickGridIdx), failGridIdx == -1 ? client : grid(failGridIdx));
                    }
                    catch (Exception e) {
                        log().error("Cannot fail node", e);
                    }

                    if (failGridIdx != -1) {
                        // After the node failure query must be restarted excluding failing node.
                        client.scheduler().runLocal(new Runnable() {
                            @Override public void run() {
                                try {
                                    qry.close();
                                }
                                catch (Exception e) {
                                    log().error("Cannot fail node", e);
                                }

                                l.countDown();
                            }
                        }, failTimeout, TimeUnit.MILLISECONDS);
                    } else
                        l.countDown();
                }
            }, failTimeout, TimeUnit.MILLISECONDS);

            try {
                // Trigger distributed execution.
                qry.iterator();
            }
            catch (Exception ex) {
                log().error("Got expected exception", ex);
            }

            l.await();

            // Give some time to clean up after query cancellation.
            Thread.sleep(3000);

            // Validate nodes query result buffer.
            checkCleanState(failGridIdx);
        }
    }

    /**
     * Validates clean state on all participating nodes after query execution stopping.
     */
    private void checkCleanState(int idx) {
        for (int i = 0; i < GRID_COUNT; i++) {
            // Skip failed server node instance check.
            if (i == idx) continue;

            IgniteEx grid = grid(i);

            // Validate everything was cleaned up.
            ConcurrentMap<UUID, ConcurrentMap<Long, ?>> map = U.field(((IgniteH2Indexing)U.field(U.field(
                grid.context(), "qryProc"), "idx")).mapQueryExecutor(), "qryRess");

            if (map.size() == 1)
                assertEquals(0, map.entrySet().iterator().next().getValue().size());
            else
                assertEquals(0, map.size());
        }
    }

    /**
     * @param victim Ignite.
     * @throws Exception In case of error.
     */
    private void failNode(Ignite mgr, Ignite victim) throws Exception {
        mgr.configuration().getDiscoverySpi().failNode(victim.cluster().localNode().id(), "Kicked by grid " + mgr.name());
    }
}