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

package org.apache.ignite.internal.metric;

import java.sql.Connection;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.spi.metric.list.MonitoringList;
import org.apache.ignite.spi.metric.list.view.CacheGroupView;
import org.apache.ignite.spi.metric.list.view.CacheView;
import org.apache.ignite.spi.metric.list.view.ClientConnectionView;
import org.apache.ignite.spi.metric.list.view.ClusterNodeView;
import org.apache.ignite.spi.metric.list.view.ContinuousQueryView;
import org.apache.ignite.spi.metric.list.view.ServiceView;
import org.apache.ignite.spi.metric.list.view.TransactionView;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext;
import org.apache.ignite.internal.processors.platform.client.ClientConnectionContext;
import org.apache.ignite.internal.processors.service.DummyService;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.util.lang.GridFunc.alwaysTrue;
import static org.apache.ignite.internal.util.lang.GridFunc.identity;
import static org.apache.ignite.spi.metric.list.view.ClusterNodeView.toStringSafe;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;

/** */
public class MonitoringListSelfTest extends GridCommonAbstractTest {
    @Test
    /** */
    public void testCachesList() throws Exception {
        try (IgniteEx g = startGrid()) {
            Set<String> cacheNames = new HashSet<>(Arrays.asList("cache-1", "cache-2"));

            for (String name : cacheNames)
                g.createCache(name);

            MonitoringList<String, CacheView> caches = g.context().metric().list("caches", "Caches", CacheView.class);

            assertEquals("ignite-sys, cache-1, cache-2", 3, F.size(caches.iterator(), alwaysTrue()));

            for (CacheView row : caches)
                cacheNames.remove(row.cacheName());

            assertTrue(cacheNames.toString(), cacheNames.isEmpty());
        }
    }

    @Test
    /** */
    public void testCacheGroupsList() throws Exception {
        try(IgniteEx g = startGrid()) {
            Set<String> grpNames = new HashSet<>(Arrays.asList("grp-1", "grp-2"));

            for (String grpName : grpNames)
                g.createCache(new CacheConfiguration<>("cache-" + grpName).setGroupName(grpName));

            MonitoringList<Integer, CacheGroupView> grps =
                g.context().metric().list("cacheGroups", "Caches group", CacheGroupView.class);

            assertEquals("ignite-sys, grp-1, grp-2", 3, F.size(grps.iterator(), alwaysTrue()));

            for (CacheGroupView row : grps)
                grpNames.remove(row.cacheGroupName());

            assertTrue(grpNames.toString(), grpNames.isEmpty());
        }
    }

    @Test
    /** */
    public void testServices() throws Exception {
        try(IgniteEx g = startGrid()) {
            ServiceConfiguration srvcCfg = new ServiceConfiguration();

            srvcCfg.setName("service");
            srvcCfg.setMaxPerNodeCount(1);
            srvcCfg.setService(new DummyService());

            g.services().deploy(srvcCfg);

            MonitoringList<IgniteUuid, ServiceView> srvs =
                g.context().metric().list("services", "Services", ServiceView.class);

            assertEquals(1, F.size(srvs.iterator(), alwaysTrue()));

            ServiceView sview = srvs.iterator().next();

            assertEquals(srvcCfg.getName(), sview.name());
            assertEquals(srvcCfg.getMaxPerNodeCount(), sview.maxPerNodeCount());
            assertEquals(DummyService.class, sview.serviceClass());
        }
    }

    @Test
    /** */
    public void testContinuousQuery() throws Exception {
        try(IgniteEx g0 = startGrid(0); IgniteEx g1 = startGrid(1)) {
            IgniteCache<Integer, Integer> cache = g0.createCache("cache-1");

            QueryCursor qry = cache.query(new ContinuousQuery<>()
                .setInitialQuery(new ScanQuery<>())
                .setPageSize(100)
                .setTimeInterval(1000)
                .setLocalListener(evts -> {
                    // No-op.
                })
                .setRemoteFilterFactory(() -> evt -> true)
            );

            for (int i=0; i<100; i++)
                cache.put(i, i);

            MonitoringList<UUID, ContinuousQueryView> qrys =
                g0.context().metric().list(metricName("query", "continuous"), "Continuous queries",
                    ContinuousQueryView.class);

            assertEquals(1, F.size(qrys.iterator(), alwaysTrue()));

            ContinuousQueryView cq = qrys.iterator().next(); //Info on originating node.

            assertEquals("cache-1", cq.cacheName());
            assertEquals(100, cq.bufferSize());
            assertEquals(1000, cq.interval());
            assertEquals(g0.localNode().id(), cq.nodeId());
            //Local listener not null on originating node.
            assertTrue(cq.localListener().startsWith(getClass().getName()));
            assertTrue(cq.remoteFilter().startsWith(getClass().getName()));
            assertNull(cq.localTransformedListener());
            assertNull(cq.remoteTransformer());

            qrys = g1.context().metric().list(metricName("query", "continuous"), "Continuous queries",
                ContinuousQueryView.class);

            assertEquals(1, F.size(qrys.iterator(), alwaysTrue()));

            cq = qrys.iterator().next(); //Info on remote node.

            assertEquals("cache-1", cq.cacheName());
            assertEquals(100, cq.bufferSize());
            assertEquals(1000, cq.interval());
            assertEquals(g0.localNode().id(), cq.nodeId());
            //Local listener is null on remote nodes.
            assertNull(cq.localListener());
            assertTrue(cq.remoteFilter().startsWith(getClass().getName()));
            assertNull(cq.localTransformedListener());
            assertNull(cq.remoteTransformer());
        }
    }

    @Test
    /** */
    public void testComputeClosures() throws Exception {
        try(IgniteEx g0 = startGrid(0)) {
            for (int i=0; i<10; i++) {
                g0.compute().run(() -> { });
            }

            MonitoringList<UUID, ContinuousQueryView> computeRunnable =
                g0.context().metric().list(metricName("compute", "runnables"), "???", ContinuousQueryView.class);
        }
    }

    @Test
    /** */
    public void testClientsConnections() throws Exception {
        try(IgniteEx g0 = startGrid(0)) {
            String host = g0.configuration().getClientConnectorConfiguration().getHost();

            if (host == null)
                host = g0.configuration().getLocalHost();

            int port = g0.configuration().getClientConnectorConfiguration().getPort();

            try (IgniteClient client =
                     Ignition.startClient(new ClientConfiguration().setAddresses(host + ":" + port))) {

                MonitoringList<Long, ClientConnectionView> conns =
                    g0.context().metric().list(metricName("client", "connections"), "Client connections",
                        ClientConnectionView.class);

                assertEquals(1, F.size(conns.iterator(), alwaysTrue()));

                ClientConnectionView cliConn = conns.iterator().next();

                assertEquals(cliConn.type(), "THIN");
                assertEquals(cliConn.localAddress().getHostName(), cliConn.remoteAddress().getHostName());
                assertEquals(g0.configuration().getClientConnectorConfiguration().getPort(),
                    cliConn.localAddress().getPort());
                assertEquals(cliConn.version(), ClientConnectionContext.DEFAULT_VER.asString());

                try(Connection conn =
                        new IgniteJdbcThinDriver().connect("jdbc:ignite:thin://" + host, new Properties())) {
                    assertEquals(2, F.size(conns.iterator(), alwaysTrue()));
                    assertEquals(1, F.size(jdbcConnectionsIterator(conns), alwaysTrue()));

                    ClientConnectionView jdbcConn = jdbcConnectionsIterator(conns).next();

                    assertEquals(jdbcConn.type(), "JDBC");
                    assertEquals(jdbcConn.localAddress().getHostName(), jdbcConn.remoteAddress().getHostName());
                    assertEquals(g0.configuration().getClientConnectorConfiguration().getPort(),
                        jdbcConn.localAddress().getPort());
                    assertEquals(jdbcConn.version(), JdbcConnectionContext.CURRENT_VER.asString());
                }
            }
        }
    }

    @Test
    /** */
    public void testTransactions() throws Exception {
        try(IgniteEx g = startGrid(0)) {
            IgniteCache<Integer, Integer> cache = g.createCache(new CacheConfiguration<Integer, Integer>("c")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

            MonitoringList<IgniteUuid, TransactionView> txs =
                g.context().metric().list(metricName("transactions"), "Transactions", TransactionView.class);

            assertEquals(0, F.size(txs.iterator(), alwaysTrue()));

            CountDownLatch latch = new CountDownLatch(1);

            try {
                AtomicInteger cntr = new AtomicInteger();

                GridTestUtils.runMultiThreadedAsync(() -> {
                    try(Transaction tx = g.transactions().withLabel("test").txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        cache.put(cntr.incrementAndGet(), cntr.incrementAndGet());
                        cache.put(cntr.incrementAndGet(), cntr.incrementAndGet());

                        latch.await();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }, 5, "xxx");

                boolean res = waitForCondition(() -> F.size(txs.iterator(), alwaysTrue()) == 5, 5_000L);

                assertTrue(res);

                TransactionView txv = txs.iterator().next();

                assertEquals(g.localNode().id(), txv.nodeId());
                assertEquals(txv.isolation(), REPEATABLE_READ);
                assertEquals(txv.concurrency(), PESSIMISTIC);
                assertEquals(txv.state(), ACTIVE);
                assertNotNull(txv.xid());
                assertFalse(txv.system());
                assertFalse(txv.implicit());
                assertFalse(txv.implicitSingle());
                assertTrue(txv.near());
                assertFalse(txv.dht());
                assertTrue(txv.colocated());
                assertTrue(txv.local());
                assertEquals("test", txv.label());
                assertFalse(txv.onePhaseCommit());
                assertFalse(txv.internal());
                assertEquals(0, txv.timeout());
                assertTrue(txv.startTime() <= System.currentTimeMillis());

                GridTestUtils.runMultiThreadedAsync(() -> {
                    try(Transaction tx = g.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                        cache.put(cntr.incrementAndGet(), cntr.incrementAndGet());
                        cache.put(cntr.incrementAndGet(), cntr.incrementAndGet());

                        latch.await();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }, 5, "xxx");

                res = waitForCondition(() -> F.size(txs.iterator(), alwaysTrue()) == 10, 5_000L);

                assertTrue(res);
            }
            finally {
                latch.countDown();
            }

            boolean res = waitForCondition(() -> F.size(txs.iterator(), alwaysTrue()) == 0, 5_000L);

            assertTrue(res);
        }
    }

    @Test
    /** */
    public void testNodes() throws Exception {
        try(IgniteEx g1 = startGrid(0)) {
            MonitoringList<UUID, ClusterNodeView> nodes =
                g1.context().metric().list("nodes", "Cluster nodes", ClusterNodeView.class);

            assertEquals(1, nodes.size());

            checkNodeView(nodes.get(g1.localNode().id()), g1.localNode(), true);

            try(IgniteEx g2 = startGrid(1)) {
                awaitPartitionMapExchange();

                assertEquals(2, nodes.size());

                checkNodeView(nodes.get(g2.localNode().id()), g2.localNode(), false);

                MonitoringList<UUID, ClusterNodeView> nodes2 =
                    g2.context().metric().list("nodes", "Cluster nodes", ClusterNodeView.class);

                assertEquals(2, nodes2.size());

                checkNodeView(nodes2.get(g1.localNode().id()), g1.localNode(), false);
                checkNodeView(nodes2.get(g2.localNode().id()), g2.localNode(), true);
            }
        }
    }

    /** */
    private void checkNodeView(ClusterNodeView n, ClusterNode loc, boolean isLocal) {
        assertEquals(loc.id(), n.id());
        assertEquals(loc.consistentId().toString(), n.consistentId());
        assertEquals(toStringSafe(loc.addresses()), n.addresses());
        assertEquals(toStringSafe(loc.hostNames()), n.hostNames());
        assertEquals(loc.order(), n.order());
        assertEquals(loc.version().toString(), n.version());
        assertEquals(isLocal, n.isLocal());
        assertEquals(loc.isDaemon(), n.isDaemon());
        assertEquals(loc.isClient(), n.isClient());
    }

    /** */
    private GridIterator<ClientConnectionView> jdbcConnectionsIterator(
        MonitoringList<Long, ClientConnectionView> conns) {
        return F.iterator(conns, identity(), true, v -> "JDBC".equals(v.type()));
    }
}
