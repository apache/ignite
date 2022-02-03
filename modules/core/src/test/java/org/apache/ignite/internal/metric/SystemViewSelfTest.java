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

import java.lang.reflect.Field;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import javax.cache.Cache;

import com.google.common.collect.Lists;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteAtomicReference;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteAtomicStamped;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteCountDownLatch;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteJdbcThinDriver;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.IgniteSemaphore;
import org.apache.ignite.IgniteSet;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.AtomicConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.mutabletest.GridBinaryTestClasses.TestObjectAllTypes;
import org.apache.ignite.internal.binary.mutabletest.GridBinaryTestClasses.TestObjectEnum;
import org.apache.ignite.internal.client.thin.ProtocolVersion;
import org.apache.ignite.internal.managers.systemview.walker.BaselineNodeAttributeViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.CachePagesListViewWalker;
import org.apache.ignite.internal.managers.systemview.walker.NodeAttributeViewWalker;
import org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager;
import org.apache.ignite.internal.processors.metastorage.DistributedMetaStorage;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcConnectionContext;
import org.apache.ignite.internal.processors.service.DummyService;
import org.apache.ignite.internal.util.StripedExecutor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.services.ServiceConfiguration;
import org.apache.ignite.spi.systemview.view.BaselineNodeAttributeView;
import org.apache.ignite.spi.systemview.view.BaselineNodeView;
import org.apache.ignite.spi.systemview.view.BinaryMetadataView;
import org.apache.ignite.spi.systemview.view.CacheGroupIoView;
import org.apache.ignite.spi.systemview.view.CacheGroupView;
import org.apache.ignite.spi.systemview.view.CachePagesListView;
import org.apache.ignite.spi.systemview.view.CacheView;
import org.apache.ignite.spi.systemview.view.ClientConnectionView;
import org.apache.ignite.spi.systemview.view.ClusterNodeView;
import org.apache.ignite.spi.systemview.view.ComputeTaskView;
import org.apache.ignite.spi.systemview.view.ContinuousQueryView;
import org.apache.ignite.spi.systemview.view.FiltrableSystemView;
import org.apache.ignite.spi.systemview.view.MetastorageView;
import org.apache.ignite.spi.systemview.view.NodeAttributeView;
import org.apache.ignite.spi.systemview.view.NodeMetricsView;
import org.apache.ignite.spi.systemview.view.PagesListView;
import org.apache.ignite.spi.systemview.view.ScanQueryView;
import org.apache.ignite.spi.systemview.view.ServiceView;
import org.apache.ignite.spi.systemview.view.SnapshotView;
import org.apache.ignite.spi.systemview.view.StripedExecutorTaskView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.spi.systemview.view.TransactionView;
import org.apache.ignite.spi.systemview.view.datastructures.AtomicLongView;
import org.apache.ignite.spi.systemview.view.datastructures.AtomicReferenceView;
import org.apache.ignite.spi.systemview.view.datastructures.AtomicSequenceView;
import org.apache.ignite.spi.systemview.view.datastructures.AtomicStampedView;
import org.apache.ignite.spi.systemview.view.datastructures.CountDownLatchView;
import org.apache.ignite.spi.systemview.view.datastructures.QueueView;
import org.apache.ignite.spi.systemview.view.datastructures.ReentrantLockView;
import org.apache.ignite.spi.systemview.view.datastructures.SemaphoreView;
import org.apache.ignite.spi.systemview.view.datastructures.SetView;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.configuration.AtomicConfiguration.DFLT_ATOMIC_SEQUENCE_RESERVE_SIZE;
import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.NODES_SYS_VIEW;
import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.NODE_ATTRIBUTES_SYS_VIEW;
import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.NODE_METRICS_SYS_VIEW;
import static org.apache.ignite.internal.managers.systemview.ScanQuerySystemView.SCAN_QRY_SYS_VIEW;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHES_VIEW;
import static org.apache.ignite.internal.processors.cache.ClusterCachesInfo.CACHE_GRPS_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.CACHE_GRP_IO_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheProcessor.CACHE_GRP_PAGE_LIST_VIEW;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheGroupId;
import static org.apache.ignite.internal.processors.cache.GridCacheUtils.cacheId;
import static org.apache.ignite.internal.processors.cache.binary.CacheObjectBinaryProcessorImpl.BINARY_METADATA_VIEW;
import static org.apache.ignite.internal.processors.cache.persistence.GridCacheDatabaseSharedManager.METASTORE_VIEW;
import static org.apache.ignite.internal.processors.cache.persistence.IgniteCacheDatabaseSharedManager.DATA_REGION_PAGE_LIST_VIEW;
import static org.apache.ignite.internal.processors.cache.persistence.metastorage.MetaStorage.METASTORAGE_CACHE_NAME;
import static org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager.TXS_MON_LIST;
import static org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor.BASELINE_NODES_SYS_VIEW;
import static org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor.BASELINE_NODE_ATTRIBUTES_SYS_VIEW;
import static org.apache.ignite.internal.processors.continuous.GridContinuousProcessor.CQ_SYS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.DEFAULT_DS_GROUP_NAME;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.DEFAULT_VOLATILE_DS_GROUP_NAME;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.LATCHES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.LOCKS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.LONGS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.QUEUES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.REFERENCES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.SEMAPHORES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.SEQUENCES_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.SETS_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.STAMPED_VIEW;
import static org.apache.ignite.internal.processors.datastructures.DataStructuresProcessor.VOLATILE_DATA_REGION_NAME;
import static org.apache.ignite.internal.processors.metastorage.persistence.DistributedMetaStorageImpl.DISTRIBUTED_METASTORE_VIEW;
import static org.apache.ignite.internal.processors.odbc.ClientListenerProcessor.CLI_CONN_VIEW;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.STREAM_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.processors.pool.PoolProcessor.SYS_POOL_QUEUE_VIEW;
import static org.apache.ignite.internal.processors.service.IgniteServiceProcessor.SVCS_VIEW;
import static org.apache.ignite.internal.processors.task.GridTaskProcessor.TASKS_VIEW;
import static org.apache.ignite.internal.util.IgniteUtils.toStringSafe;
import static org.apache.ignite.internal.util.lang.GridFunc.alwaysTrue;
import static org.apache.ignite.internal.util.lang.GridFunc.identity;
import static org.apache.ignite.spi.systemview.view.SnapshotView.SNAPSHOT_SYS_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;
import static org.apache.ignite.transactions.TransactionState.ACTIVE;

/** Tests for {@link SystemView}. */
public class SystemViewSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String TEST_PREDICATE = "TestPredicate";

    /** */
    public static final String TEST_TRANSFORMER = "TestTransformer";

    /** Tests work of {@link SystemView} for caches. */
    @Test
    public void testCachesView() throws Exception {
        try (IgniteEx g = startGrid()) {
            Set<String> cacheNames = new HashSet<>(Arrays.asList("cache-1", "cache-2"));

            for (String name : cacheNames)
                g.createCache(name);

            SystemView<CacheView> caches = g.context().systemView().view(CACHES_VIEW);

            assertEquals(g.context().cache().cacheDescriptors().size(), F.size(caches.iterator()));

            for (CacheView row : caches)
                cacheNames.remove(row.cacheName());

            assertTrue(cacheNames.toString(), cacheNames.isEmpty());
        }
    }

    /** Tests work of {@link SystemView} for cache groups. */
    @Test
    public void testCacheGroupsView() throws Exception {
        try (IgniteEx g = startGrid()) {
            Set<String> grpNames = new HashSet<>(Arrays.asList("grp-1", "grp-2"));

            for (String grpName : grpNames)
                g.createCache(new CacheConfiguration<>("cache-" + grpName).setGroupName(grpName));

            SystemView<CacheGroupView> grps = g.context().systemView().view(CACHE_GRPS_VIEW);

            assertEquals(g.context().cache().cacheGroupDescriptors().size(), F.size(grps.iterator()));

            for (CacheGroupView row : grps)
                grpNames.remove(row.cacheGroupName());

            assertTrue(grpNames.toString(), grpNames.isEmpty());
        }
    }

    /** Tests work of {@link SystemView} for services. */
    @Test
    public void testServices() throws Exception {
        try (IgniteEx g = startGrid()) {
            {
                ServiceConfiguration srvcCfg = new ServiceConfiguration();

                srvcCfg.setName("service");
                srvcCfg.setMaxPerNodeCount(1);
                srvcCfg.setService(new DummyService());
                srvcCfg.setNodeFilter(new TestNodeFilter());

                g.services().deploy(srvcCfg);

                SystemView<ServiceView> srvs = g.context().systemView().view(SVCS_VIEW);

                assertEquals(g.context().service().serviceDescriptors().size(), F.size(srvs.iterator()));

                ServiceView sview = srvs.iterator().next();

                assertEquals(srvcCfg.getName(), sview.name());
                assertNotNull(sview.serviceId());
                assertEquals(srvcCfg.getMaxPerNodeCount(), sview.maxPerNodeCount());
                assertEquals(DummyService.class, sview.serviceClass());
                assertEquals(srvcCfg.getMaxPerNodeCount(), sview.maxPerNodeCount());
                assertNull(sview.cacheName());
                assertNull(sview.affinityKey());
                assertEquals(TestNodeFilter.class, sview.nodeFilter());
                assertFalse(sview.staticallyConfigured());
                assertEquals(g.localNode().id(), sview.originNodeId());
            }

            {
                g.createCache("test-cache");

                ServiceConfiguration srvcCfg = new ServiceConfiguration();

                srvcCfg.setName("service-2");
                srvcCfg.setMaxPerNodeCount(2);
                srvcCfg.setService(new DummyService());
                srvcCfg.setNodeFilter(new TestNodeFilter());
                srvcCfg.setCacheName("test-cache");
                srvcCfg.setAffinityKey(1L);

                g.services().deploy(srvcCfg);

                final ServiceView[] sview = {null};

                g.context().systemView().<ServiceView>view(SVCS_VIEW).forEach(sv -> {
                    if (sv.name().equals(srvcCfg.getName()))
                        sview[0] = sv;
                });

                assertEquals(srvcCfg.getName(), sview[0].name());
                assertNotNull(sview[0].serviceId());
                assertEquals(srvcCfg.getMaxPerNodeCount(), sview[0].maxPerNodeCount());
                assertEquals(DummyService.class, sview[0].serviceClass());
                assertEquals(srvcCfg.getMaxPerNodeCount(), sview[0].maxPerNodeCount());
                assertEquals("test-cache", sview[0].cacheName());
                assertEquals("1", sview[0].affinityKey());
                assertEquals(TestNodeFilter.class, sview[0].nodeFilter());
                assertFalse(sview[0].staticallyConfigured());
                assertEquals(g.localNode().id(), sview[0].originNodeId());
            }
        }
    }

    /** Tests work of {@link SystemView} for compute grid {@link IgniteCompute#broadcastAsync(IgniteRunnable)} call. */
    @Test
    public void testComputeBroadcast() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(6);

        try (IgniteEx g1 = startGrid(0)) {
            SystemView<ComputeTaskView> tasks = g1.context().systemView().view(TASKS_VIEW);

            for (int i = 0; i < 5; i++) {
                g1.compute().broadcastAsync(() -> {
                    try {
                        barrier.await();
                        barrier.await();
                    }
                    catch (InterruptedException | BrokenBarrierException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            barrier.await();

            assertEquals(5, tasks.size());

            ComputeTaskView t = tasks.iterator().next();

            assertFalse(t.internal());
            assertNull(t.affinityCacheName());
            assertEquals(-1, t.affinityPartitionId());
            assertTrue(t.taskClassName().startsWith(getClass().getName()));
            assertTrue(t.taskName().startsWith(getClass().getName()));
            assertEquals(g1.localNode().id(), t.taskNodeId());
            assertEquals("0", t.userVersion());

            barrier.await();
        }
    }

    /** Tests work of {@link SystemView} for compute grid {@link IgniteCompute#runAsync(IgniteRunnable)} call. */
    @Test
    public void testComputeRunnable() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);

        try (IgniteEx g1 = startGrid(0)) {
            SystemView<ComputeTaskView> tasks = g1.context().systemView().view(TASKS_VIEW);

            g1.compute().runAsync(() -> {
                try {
                    barrier.await();
                    barrier.await();
                }
                catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
            });

            barrier.await();

            assertEquals(1, tasks.size());

            ComputeTaskView t = tasks.iterator().next();

            assertFalse(t.internal());
            assertNull(t.affinityCacheName());
            assertEquals(-1, t.affinityPartitionId());
            assertTrue(t.taskClassName().startsWith(getClass().getName()));
            assertTrue(t.taskName().startsWith(getClass().getName()));
            assertEquals(g1.localNode().id(), t.taskNodeId());
            assertEquals("0", t.userVersion());

            barrier.await();
        }
    }

    /** Tests work of {@link SystemView} for compute grid {@link IgniteCompute#apply(IgniteClosure, Object)} call. */
    @Test
    public void testComputeApply() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);

        try (IgniteEx g1 = startGrid(0)) {
            SystemView<ComputeTaskView> tasks = g1.context().systemView().view(TASKS_VIEW);

            GridTestUtils.runAsync(() -> {
                g1.compute().apply(x -> {
                    try {
                        barrier.await();
                        barrier.await();
                    }
                    catch (InterruptedException | BrokenBarrierException e) {
                        throw new RuntimeException(e);
                    }

                    return 0;
                }, 1);
            });

            barrier.await();

            assertEquals(1, tasks.size());

            ComputeTaskView t = tasks.iterator().next();

            assertFalse(t.internal());
            assertNull(t.affinityCacheName());
            assertEquals(-1, t.affinityPartitionId());
            assertTrue(t.taskClassName().startsWith(getClass().getName()));
            assertTrue(t.taskName().startsWith(getClass().getName()));
            assertEquals(g1.localNode().id(), t.taskNodeId());
            assertEquals("0", t.userVersion());

            barrier.await();
        }
    }

    /**
     * Tests work of {@link SystemView} for compute grid
     * {@link IgniteCompute#affinityCallAsync(String, Object, IgniteCallable)} call.
     */
    @Test
    public void testComputeAffinityCall() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);

        try (IgniteEx g1 = startGrid(0)) {
            SystemView<ComputeTaskView> tasks = g1.context().systemView().view(TASKS_VIEW);

            IgniteCache<Integer, Integer> cache = g1.createCache("test-cache");

            cache.put(1, 1);

            g1.compute().affinityCallAsync("test-cache", 1, () -> {
                try {
                    barrier.await();
                    barrier.await();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                return 0;
            });

            barrier.await();

            assertEquals(1, tasks.size());

            ComputeTaskView t = tasks.iterator().next();

            assertFalse(t.internal());
            assertEquals("test-cache", t.affinityCacheName());
            assertEquals(1, t.affinityPartitionId());
            assertTrue(t.taskClassName().startsWith(getClass().getName()));
            assertTrue(t.taskName().startsWith(getClass().getName()));
            assertEquals(g1.localNode().id(), t.taskNodeId());
            assertEquals("0", t.userVersion());

            barrier.await();
        }
    }

    /** */
    @Test
    public void testComputeTask() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);

        try (IgniteEx g1 = startGrid(0)) {
            SystemView<ComputeTaskView> tasks = g1.context().systemView().view(TASKS_VIEW);

            IgniteCache<Integer, Integer> cache = g1.createCache("test-cache");

            cache.put(1, 1);

            g1.compute().executeAsync(new ComputeTask<Object, Object>() {
                @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
                    @Nullable Object arg) throws IgniteException {
                    return Collections.singletonMap(new ComputeJob() {
                        @Override public void cancel() {
                            // No-op.
                        }

                        @Override public Object execute() throws IgniteException {
                            return 1;
                        }
                    }, subgrid.get(0));
                }

                @Override public ComputeJobResultPolicy result(ComputeJobResult res,
                    List<ComputeJobResult> rcvd) throws IgniteException {
                    try {
                        barrier.await();
                        barrier.await();
                    }
                    catch (InterruptedException | BrokenBarrierException e) {
                        throw new RuntimeException(e);
                    }

                    return null;
                }

                @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
                    return 1;
                }
            }, 1);

            barrier.await();

            assertEquals(1, tasks.size());

            ComputeTaskView t = tasks.iterator().next();

            assertFalse(t.internal());
            assertNull(t.affinityCacheName());
            assertEquals(-1, t.affinityPartitionId());
            assertTrue(t.taskClassName().startsWith(getClass().getName()));
            assertTrue(t.taskName().startsWith(getClass().getName()));
            assertEquals(g1.localNode().id(), t.taskNodeId());
            assertEquals("0", t.userVersion());

            barrier.await();
        }
    }

    /** */
    @Test
    public void testClientsConnections() throws Exception {
        try (IgniteEx g0 = startGrid(0)) {
            String host = g0.configuration().getClientConnectorConfiguration().getHost();

            if (host == null)
                host = g0.configuration().getLocalHost();

            int port = g0.configuration().getClientConnectorConfiguration().getPort();

            SystemView<ClientConnectionView> conns = g0.context().systemView().view(CLI_CONN_VIEW);

            try (IgniteClient cli = Ignition.startClient(new ClientConfiguration().setAddresses(host + ":" + port))) {
                assertEquals(1, conns.size());

                ClientConnectionView cliConn = conns.iterator().next();

                assertEquals("THIN", cliConn.type());
                assertEquals(cliConn.localAddress().getHostName(), cliConn.remoteAddress().getHostName());
                assertEquals(g0.configuration().getClientConnectorConfiguration().getPort(),
                    cliConn.localAddress().getPort());
                assertEquals(cliConn.version(), ProtocolVersion.LATEST_VER.toString());

                try (Connection conn =
                         new IgniteJdbcThinDriver().connect("jdbc:ignite:thin://" + host, new Properties())) {
                    assertEquals(2, conns.size());
                    assertEquals(1, F.size(jdbcConnectionsIterator(conns)));

                    ClientConnectionView jdbcConn = jdbcConnectionsIterator(conns).next();

                    assertEquals("JDBC", jdbcConn.type());
                    assertEquals(jdbcConn.localAddress().getHostName(), jdbcConn.remoteAddress().getHostName());
                    assertEquals(g0.configuration().getClientConnectorConfiguration().getPort(),
                        jdbcConn.localAddress().getPort());
                    assertEquals(jdbcConn.version(), JdbcConnectionContext.CURRENT_VER.asString());
                }
            }

            boolean res = GridTestUtils.waitForCondition(() -> conns.size() == 0, 5_000);

            assertTrue(res);
        }
    }

    /** */
    @Test
    public void testContinuousQuery() throws Exception {
        try (IgniteEx originNode = startGrid(0); IgniteEx remoteNode = startGrid(1)) {
            IgniteCache<Integer, Integer> cache = originNode.createCache("cache-1");

            SystemView<ContinuousQueryView> origQrys = originNode.context().systemView().view(CQ_SYS_VIEW);
            SystemView<ContinuousQueryView> remoteQrys = remoteNode.context().systemView().view(CQ_SYS_VIEW);

            assertEquals(0, origQrys.size());
            assertEquals(0, remoteQrys.size());

            try (QueryCursor qry = cache.query(new ContinuousQuery<>()
                .setInitialQuery(new ScanQuery<>())
                .setPageSize(100)
                .setTimeInterval(1000)
                .setLocalListener(evts -> {
                    // No-op.
                })
                .setRemoteFilterFactory(() -> evt -> true)
            )) {
                for (int i = 0; i < 100; i++)
                    cache.put(i, i);

                checkContinuousQueryView(originNode, origQrys, true);
                checkContinuousQueryView(originNode, remoteQrys, false);
            }

            assertEquals(0, origQrys.size());
            assertTrue(waitForCondition(() -> remoteQrys.size() == 0, getTestTimeout()));
        }
    }

    /** */
    private void checkContinuousQueryView(IgniteEx g, SystemView<ContinuousQueryView> qrys, boolean loc) {
        assertEquals(1, qrys.size());

        for (ContinuousQueryView cq : qrys) {
            assertEquals("cache-1", cq.cacheName());
            assertEquals(100, cq.bufferSize());
            assertEquals(1000, cq.interval());
            assertEquals(g.localNode().id(), cq.nodeId());

            if (loc)
                assertTrue(cq.localListener().startsWith(getClass().getName()));
            else
                assertNull(cq.localListener());

            assertTrue(cq.remoteFilter().startsWith(getClass().getName()));
            assertNull(cq.localTransformedListener());
            assertNull(cq.remoteTransformer());
        }
    }

    /** */
    @Test
    public void testNodes() throws Exception {
        try (IgniteEx g1 = startGrid(0)) {
            SystemView<ClusterNodeView> views = g1.context().systemView().view(NODES_SYS_VIEW);

            assertEquals(1, views.size());

            try (IgniteEx g2 = startGrid(1)) {
                awaitPartitionMapExchange();

                checkViewsState(views, g1.localNode(), g2.localNode());
                checkViewsState(g2.context().systemView().view(NODES_SYS_VIEW), g2.localNode(), g1.localNode());

            }

            assertEquals(1, views.size());
        }
    }

    /** */
    @Test
    public void testNodeAttributes() throws Exception {
        try (
            IgniteEx ignite0 = startGrid(getConfiguration(getTestIgniteInstanceName(0))
                .setUserAttributes(F.asMap("name", "val0")));
            IgniteEx ignite1 = startGrid(getConfiguration(getTestIgniteInstanceName(1))
                .setUserAttributes(F.asMap("name", "val1")))
        ) {
            awaitPartitionMapExchange();

            SystemView<NodeAttributeView> view = ignite0.context().systemView().view(NODE_ATTRIBUTES_SYS_VIEW);

            assertEquals(ignite0.cluster().localNode().attributes().size() +
                ignite1.cluster().localNode().attributes().size(), view.size());

            assertEquals(1, F.size(view.iterator(), row -> "name".equals(row.name()) && "val0".equals(row.value())));
            assertEquals(1, F.size(view.iterator(), row -> "name".equals(row.name()) && "val1".equals(row.value())));

            // Test filtering.
            assertTrue(view instanceof FiltrableSystemView);

            Iterator<NodeAttributeView> iter = ((FiltrableSystemView<NodeAttributeView>)view)
                .iterator(F.asMap(NodeAttributeViewWalker.NODE_ID_FILTER, ignite0.cluster().localNode().id()));

            assertEquals(1, F.size(iter, row -> "name".equals(row.name()) && "val0".equals(row.value())));

            iter = ((FiltrableSystemView<NodeAttributeView>)view).iterator(
                F.asMap(NodeAttributeViewWalker.NODE_ID_FILTER, ignite1.cluster().localNode().id().toString()));

            assertEquals(1, F.size(iter, row -> "name".equals(row.name()) && "val1".equals(row.value())));

            iter = ((FiltrableSystemView<NodeAttributeView>)view).iterator(
                F.asMap(NodeAttributeViewWalker.NODE_ID_FILTER, "malformed-id"));

            assertEquals(0, F.size(iter));

            iter = ((FiltrableSystemView<NodeAttributeView>)view).iterator(
                F.asMap(NodeAttributeViewWalker.NAME_FILTER, "name"));

            assertEquals(2, F.size(iter));

            iter = ((FiltrableSystemView<NodeAttributeView>)view)
                .iterator(F.asMap(NodeAttributeViewWalker.NODE_ID_FILTER, ignite0.cluster().localNode().id(),
                    NodeAttributeViewWalker.NAME_FILTER, "name"));

            assertEquals(1, F.size(iter));
        }
    }

    /** */
    @Test
    public void testNodeMetrics() throws Exception {
        long ts = U.currentTimeMillis();

        try (IgniteEx ignite0 = startGrid(0); IgniteEx ignite1 = startGrid(1)) {
            awaitPartitionMapExchange();

            SystemView<NodeMetricsView> view = ignite0.context().systemView().view(NODE_METRICS_SYS_VIEW);

            assertEquals(2, view.size());
            assertEquals(1, F.size(view.iterator(), row -> row.nodeId().equals(ignite0.cluster().localNode().id())));
            assertEquals(1, F.size(view.iterator(), row -> row.nodeId().equals(ignite1.cluster().localNode().id())));
            assertEquals(2, F.size(view.iterator(), row -> row.lastUpdateTime().getTime() >= ts));
        }
    }

    /** */
    @Test
    public void testCacheGroupIo() throws Exception {
        cleanPersistenceDir();

        try (IgniteEx ignite = startGrid(getConfiguration().setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true))))
        ) {
            ignite.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Object, Object> cache = ignite.createCache("cache");

            cache.put(0, 0);
            cache.get(0);

            SystemView<CacheGroupIoView> view = ignite.context().systemView().view(CACHE_GRP_IO_VIEW);

            CacheGroupIoView row = F.find(view, null,
                (IgnitePredicate<CacheGroupIoView>)r -> "cache".equals(r.cacheGroupName()));

            assertNotNull(row);
            assertTrue(row.logicalReads() > 0);
        }
    }

    /** */
    private void checkViewsState(SystemView<ClusterNodeView> views, ClusterNode loc, ClusterNode rmt) {
        assertEquals(2, views.size());

        for (ClusterNodeView nodeView : views) {
            if (nodeView.nodeId().equals(loc.id()))
                checkNodeView(nodeView, loc, true);
            else
                checkNodeView(nodeView, rmt, false);
        }
    }

    /** */
    private void checkNodeView(ClusterNodeView view, ClusterNode node, boolean isLoc) {
        assertEquals(node.id(), view.nodeId());
        assertEquals(node.consistentId().toString(), view.consistentId());
        assertEquals(toStringSafe(node.addresses()), view.addresses());
        assertEquals(toStringSafe(node.hostNames()), view.hostnames());
        assertEquals(node.order(), view.nodeOrder());
        assertEquals(node.version().toString(), view.version());
        assertEquals(isLoc, view.isLocal());
        assertEquals(node.isDaemon(), view.isDaemon());
        assertEquals(node.isClient(), view.isClient());
    }

    /** */
    private Iterator<ClientConnectionView> jdbcConnectionsIterator(SystemView<ClientConnectionView> conns) {
        return F.iterator(conns.iterator(), identity(), true, v -> "JDBC".equals(v.type()));
    }

    /** */
    @Test
    public void testTransactions() throws Exception {
        try (IgniteEx g = startGrid(0)) {
            IgniteCache<Integer, Integer> cache1 = g.createCache(new CacheConfiguration<Integer, Integer>("c1")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

            IgniteCache<Integer, Integer> cache2 = g.createCache(new CacheConfiguration<Integer, Integer>("c2")
                .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL));

            SystemView<TransactionView> txs = g.context().systemView().view(TXS_MON_LIST);

            assertEquals(0, F.size(txs.iterator(), alwaysTrue()));

            CountDownLatch latch = new CountDownLatch(1);

            try {
                AtomicInteger cntr = new AtomicInteger();

                GridTestUtils.runMultiThreadedAsync(() -> {
                    try (Transaction tx = g.transactions().withLabel("test").txStart(PESSIMISTIC, REPEATABLE_READ)) {
                        cache1.put(cntr.incrementAndGet(), cntr.incrementAndGet());
                        cache1.put(cntr.incrementAndGet(), cntr.incrementAndGet());

                        latch.await();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }, 5, "xxx");

                boolean res = waitForCondition(() -> txs.size() == 5, 10_000L);

                assertTrue(res);

                TransactionView txv = txs.iterator().next();

                assertEquals(g.localNode().id(), txv.localNodeId());
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
                assertEquals(String.valueOf(cacheId(cache1.getName())), txv.cacheIds());

                //Only pessimistic transactions are supported when MVCC is enabled.
                if (Objects.equals(System.getProperty(IgniteSystemProperties.IGNITE_FORCE_MVCC_MODE_IN_TESTS), "true"))
                    return;

                GridTestUtils.runMultiThreadedAsync(() -> {
                    try (Transaction tx = g.transactions().txStart(OPTIMISTIC, SERIALIZABLE)) {
                        cache1.put(cntr.incrementAndGet(), cntr.incrementAndGet());
                        cache1.put(cntr.incrementAndGet(), cntr.incrementAndGet());
                        cache2.put(cntr.incrementAndGet(), cntr.incrementAndGet());

                        latch.await();
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }, 5, "xxx");

                res = waitForCondition(() -> txs.size() == 10, 10_000L);

                assertTrue(res);

                for (TransactionView tx : txs) {
                    if (PESSIMISTIC == tx.concurrency())
                        continue;

                    assertEquals(g.localNode().id(), tx.localNodeId());
                    assertEquals(tx.isolation(), SERIALIZABLE);
                    assertEquals(tx.concurrency(), OPTIMISTIC);
                    assertEquals(tx.state(), ACTIVE);
                    assertNotNull(tx.xid());
                    assertFalse(tx.system());
                    assertFalse(tx.implicit());
                    assertFalse(tx.implicitSingle());
                    assertTrue(tx.near());
                    assertFalse(tx.dht());
                    assertTrue(tx.colocated());
                    assertTrue(tx.local());
                    assertNull(tx.label());
                    assertFalse(tx.onePhaseCommit());
                    assertFalse(tx.internal());
                    assertEquals(0, tx.timeout());
                    assertTrue(tx.startTime() <= System.currentTimeMillis());

                    String s1 = cacheId(cache1.getName()) + "," + cacheId(cache2.getName());
                    String s2 = cacheId(cache2.getName()) + "," + cacheId(cache1.getName());

                    assertTrue(s1.equals(tx.cacheIds()) || s2.equals(tx.cacheIds()));
                }
            }
            finally {
                latch.countDown();
            }

            boolean res = waitForCondition(() -> txs.size() == 0, 10_000L);

            assertTrue(res);
        }
    }

    /** */
    @Test
    public void testLocalScanQuery() throws Exception {
        try (IgniteEx g0 = startGrid(0)) {
            IgniteCache<Integer, Integer> cache1 = g0.createCache(
                new CacheConfiguration<Integer, Integer>("cache1")
                    .setGroupName("group1"));

            int part = g0.affinity("cache1").primaryPartitions(g0.localNode())[0];

            List<Integer> partKeys = partitionKeys(cache1, part, 11, 0);

            for (Integer key : partKeys)
                cache1.put(key, key);

            SystemView<ScanQueryView> qrySysView0 = g0.context().systemView().view(SCAN_QRY_SYS_VIEW);

            assertNotNull(qrySysView0);

            assertEquals(0, qrySysView0.size());

            QueryCursor<Integer> qryRes1 = cache1.query(
                new ScanQuery<Integer, Integer>()
                    .setFilter(new TestPredicate())
                    .setLocal(true)
                    .setPartition(part)
                    .setPageSize(10),
                new TestTransformer());

            assertTrue(qryRes1.iterator().hasNext());

            boolean res = waitForCondition(() -> qrySysView0.size() > 0, 5_000);

            assertTrue(res);

            ScanQueryView view = qrySysView0.iterator().next();

            assertEquals(g0.localNode().id(), view.originNodeId());
            assertEquals(0, view.queryId());
            assertEquals("cache1", view.cacheName());
            assertEquals(cacheId("cache1"), view.cacheId());
            assertEquals(cacheGroupId("cache1", "group1"), view.cacheGroupId());
            assertEquals("group1", view.cacheGroupName());
            assertTrue(view.startTime() <= System.currentTimeMillis());
            assertTrue(view.duration() >= 0);
            assertFalse(view.canceled());
            assertEquals(TEST_PREDICATE, view.filter());
            assertTrue(view.local());
            assertEquals(part, view.partition());
            assertEquals(toStringSafe(g0.context().discovery().topologyVersionEx()), view.topology());
            assertEquals(TEST_TRANSFORMER, view.transformer());
            assertFalse(view.keepBinary());
            assertNull(view.subjectId());
            assertNull(view.taskName());

            qryRes1.close();

            res = waitForCondition(() -> qrySysView0.size() == 0, 5_000);

            assertTrue(res);
        }
    }

    /** */
    @Test
    public void testScanQuery() throws Exception {
        try (IgniteEx g0 = startGrid(0);
            IgniteEx g1 = startGrid(1);
            IgniteEx client1 = startClientGrid("client-1");
            IgniteEx client2 = startClientGrid("client-2")) {

            IgniteCache<Integer, Integer> cache1 = client1.createCache(
                new CacheConfiguration<Integer, Integer>("cache1")
                    .setGroupName("group1"));

            IgniteCache<Integer, Integer> cache2 = client2.createCache("cache2");

            for (int i = 0; i < 100; i++) {
                cache1.put(i, i);
                cache2.put(i, i);
            }

            SystemView<ScanQueryView> qrySysView0 = g0.context().systemView().view(SCAN_QRY_SYS_VIEW);
            SystemView<ScanQueryView> qrySysView1 = g1.context().systemView().view(SCAN_QRY_SYS_VIEW);

            assertNotNull(qrySysView0);
            assertNotNull(qrySysView1);

            assertEquals(0, qrySysView0.size());
            assertEquals(0, qrySysView1.size());

            QueryCursor<Integer> qryRes1 = cache1.query(
                new ScanQuery<Integer, Integer>()
                    .setFilter(new TestPredicate())
                    .setPageSize(10),
                new TestTransformer());

            QueryCursor<?> qryRes2 = cache2.withKeepBinary().query(new ScanQuery<>()
                .setPageSize(20));

            assertTrue(qryRes1.iterator().hasNext());
            assertTrue(qryRes2.iterator().hasNext());

            checkScanQueryView(client1, client2, qrySysView0);
            checkScanQueryView(client1, client2, qrySysView1);

            qryRes1.close();
            qryRes2.close();

            boolean res = waitForCondition(
                () -> qrySysView0.size() + qrySysView1.size() == 0, 5_000);

            assertTrue(res);
        }
    }

    /** */
    private void checkScanQueryView(IgniteEx client1, IgniteEx client2, SystemView<ScanQueryView> qrySysView)
        throws Exception {
        boolean res = waitForCondition(() -> qrySysView.size() > 1, 5_000);

        assertTrue(res);

        Consumer<ScanQueryView> cache1checker = view -> {
            assertEquals(client1.localNode().id(), view.originNodeId());
            assertTrue(view.queryId() != 0);
            assertEquals("cache1", view.cacheName());
            assertEquals(cacheId("cache1"), view.cacheId());
            assertEquals(cacheGroupId("cache1", "group1"), view.cacheGroupId());
            assertEquals("group1", view.cacheGroupName());
            assertTrue(view.startTime() <= System.currentTimeMillis());
            assertTrue(view.duration() >= 0);
            assertFalse(view.canceled());
            assertEquals(TEST_PREDICATE, view.filter());
            assertFalse(view.local());
            assertEquals(-1, view.partition());
            assertEquals(toStringSafe(client1.context().discovery().topologyVersionEx()), view.topology());
            assertEquals(TEST_TRANSFORMER, view.transformer());
            assertFalse(view.keepBinary());
            assertNull(view.subjectId());
            assertNull(view.taskName());
            assertEquals(10, view.pageSize());
        };

        Consumer<ScanQueryView> cache2checker = view -> {
            assertEquals(client2.localNode().id(), view.originNodeId());
            assertTrue(view.queryId() != 0);
            assertEquals("cache2", view.cacheName());
            assertEquals(cacheId("cache2"), view.cacheId());
            assertEquals(cacheGroupId("cache2", null), view.cacheGroupId());
            assertEquals("cache2", view.cacheGroupName());
            assertTrue(view.startTime() <= System.currentTimeMillis());
            assertTrue(view.duration() >= 0);
            assertFalse(view.canceled());
            assertNull(view.filter());
            assertFalse(view.local());
            assertEquals(-1, view.partition());
            assertEquals(toStringSafe(client2.context().discovery().topologyVersionEx()), view.topology());
            assertNull(view.transformer());
            assertTrue(view.keepBinary());
            assertNull(view.subjectId());
            assertNull(view.taskName());
            assertEquals(20, view.pageSize());
        };

        boolean found1 = false;
        boolean found2 = false;

        for (ScanQueryView view : qrySysView) {
            if ("cache2".equals(view.cacheName())) {
                cache2checker.accept(view);
                found1 = true;
            }
            else {
                cache1checker.accept(view);
                found2 = true;
            }
        }

        assertTrue(found1 && found2);
    }

    /** */
    @Test
    public void testAtomicSequence() throws Exception {
        try (IgniteEx g0 = startGrid(0);
             IgniteEx g1 = startGrid(1)) {
            IgniteAtomicSequence s1 = g0.atomicSequence("seq-1", 42, true);
            IgniteAtomicSequence s2 = g0.atomicSequence("seq-2",
                new AtomicConfiguration().setBackups(1).setGroupName("my-group"), 43, true);

            s1.batchSize(42);

            SystemView<AtomicSequenceView> seqs0 = g0.context().systemView().view(SEQUENCES_VIEW);
            SystemView<AtomicSequenceView> seqs1 = g1.context().systemView().view(SEQUENCES_VIEW);

            assertEquals(2, seqs0.size());
            assertEquals(0, seqs1.size());

            for (AtomicSequenceView s : seqs0) {
                if ("seq-1".equals(s.name())) {
                    assertEquals(42, s.value());
                    assertEquals(42, s.batchSize());
                    assertEquals(DEFAULT_DS_GROUP_NAME, s.groupName());
                    assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), s.groupId());

                    long val = s1.addAndGet(42);

                    assertEquals(val, s.value());
                    assertFalse(s.removed());
                }
                else {
                    assertEquals("seq-2", s.name());
                    assertEquals(DFLT_ATOMIC_SEQUENCE_RESERVE_SIZE, s.batchSize());
                    assertEquals(43, s.value());
                    assertEquals("my-group", s.groupName());
                    assertEquals(CU.cacheId("my-group"), s.groupId());
                    assertFalse(s.removed());

                    s2.close();

                    assertTrue(waitForCondition(s::removed, getTestTimeout()));
                }
            }

            g1.atomicSequence("seq-1", 42, true);

            assertEquals(1, seqs1.size());

            AtomicSequenceView s = seqs1.iterator().next();

            assertEquals(DFLT_ATOMIC_SEQUENCE_RESERVE_SIZE + 42, s.value());
            assertEquals(DFLT_ATOMIC_SEQUENCE_RESERVE_SIZE, s.batchSize());
            assertEquals(DEFAULT_DS_GROUP_NAME, s.groupName());
            assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), s.groupId());
            assertFalse(s.removed());

            s1.close();

            assertTrue(waitForCondition(s::removed, getTestTimeout()));

            assertEquals(0, seqs0.size());
            assertEquals(0, seqs1.size());
        }
    }

    /** */
    @Test
    public void testAtomicLongs() throws Exception {
        try (IgniteEx g0 = startGrid(0);
             IgniteEx g1 = startGrid(1)) {
            IgniteAtomicLong l1 = g0.atomicLong("long-1", 42, true);
            IgniteAtomicLong l2 = g0.atomicLong("long-2",
                new AtomicConfiguration().setBackups(1).setGroupName("my-group"), 43, true);

            SystemView<AtomicLongView> longs0 = g0.context().systemView().view(LONGS_VIEW);
            SystemView<AtomicLongView> longs1 = g1.context().systemView().view(LONGS_VIEW);

            assertEquals(2, longs0.size());
            assertEquals(0, longs1.size());

            for (AtomicLongView l : longs0) {
                if ("long-1".equals(l.name())) {
                    assertEquals(42, l.value());
                    assertEquals(DEFAULT_DS_GROUP_NAME, l.groupName());
                    assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), l.groupId());

                    long val = l1.addAndGet(42);

                    assertEquals(val, l.value());
                    assertFalse(l.removed());
                }
                else {
                    assertEquals("long-2", l.name());
                    assertEquals(43, l.value());
                    assertEquals("my-group", l.groupName());
                    assertEquals(CU.cacheId("my-group"), l.groupId());
                    assertFalse(l.removed());

                    l2.close();

                    assertTrue(waitForCondition(l::removed, getTestTimeout()));
                }
            }

            g1.atomicLong("long-1", 42, true);

            assertEquals(1, longs1.size());

            AtomicLongView l = longs1.iterator().next();

            assertEquals(84, l.value());
            assertEquals(DEFAULT_DS_GROUP_NAME, l.groupName());
            assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), l.groupId());
            assertFalse(l.removed());

            l1.close();

            assertTrue(waitForCondition(l::removed, getTestTimeout()));

            assertEquals(0, longs0.size());
            assertEquals(0, longs1.size());
        }
    }

    /** */
    @Test
    public void testAtomicReference() throws Exception {
        try (IgniteEx g0 = startGrid(0);
             IgniteEx g1 = startGrid(1)) {
            IgniteAtomicReference<String> l1 = g0.atomicReference("ref-1", "str1", true);
            IgniteAtomicReference<Integer> l2 = g0.atomicReference("ref-2",
                new AtomicConfiguration().setBackups(1).setGroupName("my-group"), 43, true);

            SystemView<AtomicReferenceView> refs0 = g0.context().systemView().view(REFERENCES_VIEW);
            SystemView<AtomicReferenceView> refs1 = g1.context().systemView().view(REFERENCES_VIEW);

            assertEquals(2, refs0.size());
            assertEquals(0, refs1.size());

            for (AtomicReferenceView r : refs0) {
                if ("ref-1".equals(r.name())) {
                    assertEquals("str1", r.value());
                    assertEquals(DEFAULT_DS_GROUP_NAME, r.groupName());
                    assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), r.groupId());

                    l1.set("str2");

                    assertEquals("str2", r.value());
                    assertFalse(r.removed());
                }
                else {
                    assertEquals("ref-2", r.name());
                    assertEquals("43", r.value());
                    assertEquals("my-group", r.groupName());
                    assertEquals(CU.cacheId("my-group"), r.groupId());
                    assertFalse(r.removed());

                    l2.close();

                    assertTrue(waitForCondition(r::removed, getTestTimeout()));
                }
            }

            g1.atomicReference("ref-1", "str3", true);

            assertEquals(1, refs1.size());

            AtomicReferenceView l = refs1.iterator().next();

            assertEquals("str2", l.value());
            assertEquals(DEFAULT_DS_GROUP_NAME, l.groupName());
            assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), l.groupId());
            assertFalse(l.removed());

            l1.close();

            assertTrue(waitForCondition(l::removed, getTestTimeout()));

            assertEquals(0, refs0.size());
            assertEquals(0, refs1.size());
        }
    }

    /** */
    @Test
    public void testAtomicStamped() throws Exception {
        try (IgniteEx g0 = startGrid(0);
             IgniteEx g1 = startGrid(1)) {
            IgniteAtomicStamped<String, Integer> s1 = g0.atomicStamped("s-1", "str0", 1, true);
            IgniteAtomicStamped<String, Integer> s2 = g0.atomicStamped("s-2",
                new AtomicConfiguration().setBackups(1).setGroupName("my-group"), "str1", 43, true);

            SystemView<AtomicStampedView> stamps0 = g0.context().systemView().view(STAMPED_VIEW);
            SystemView<AtomicStampedView> stamps1 = g1.context().systemView().view(STAMPED_VIEW);

            assertEquals(2, stamps0.size());
            assertEquals(0, stamps1.size());

            for (AtomicStampedView s : stamps0) {
                if ("s-1".equals(s.name())) {
                    assertEquals("str0", s.value());
                    assertEquals("1", s.stamp());
                    assertEquals(DEFAULT_DS_GROUP_NAME, s.groupName());
                    assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), s.groupId());

                    s1.set("str2", 2);

                    assertEquals("str2", s.value());
                    assertEquals("2", s.stamp());
                    assertFalse(s.removed());
                }
                else {
                    assertEquals("s-2", s.name());
                    assertEquals("str1", s.value());
                    assertEquals("43", s.stamp());
                    assertEquals("my-group", s.groupName());
                    assertEquals(CU.cacheId("my-group"), s.groupId());
                    assertFalse(s.removed());

                    s2.close();

                    assertTrue(waitForCondition(s::removed, getTestTimeout()));
                }
            }

            g1.atomicStamped("s-1", "str3", 3, true);

            assertEquals(1, stamps1.size());

            AtomicStampedView l = stamps1.iterator().next();

            assertEquals("str2", l.value());
            assertEquals("2", l.stamp());
            assertEquals(DEFAULT_DS_GROUP_NAME, l.groupName());
            assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), l.groupId());
            assertFalse(l.removed());

            s1.close();

            assertTrue(l.removed());

            assertEquals(0, stamps0.size());
            assertEquals(0, stamps1.size());
        }
    }

    /** */
    @Test
    public void testCountDownLatch() throws Exception {
        try (IgniteEx g0 = startGrid(0);
             IgniteEx g1 = startGrid(1)) {
            IgniteCountDownLatch l1 = g0.countDownLatch("c1", 3, false, true);
            IgniteCountDownLatch l2 = g0.countDownLatch("c2", 1, true, true);

            SystemView<CountDownLatchView> latches0 = g0.context().systemView().view(LATCHES_VIEW);
            SystemView<CountDownLatchView> latches1 = g1.context().systemView().view(LATCHES_VIEW);

            assertEquals(2, latches0.size());
            assertEquals(0, latches1.size());

            String grpName = DEFAULT_VOLATILE_DS_GROUP_NAME + "@" + VOLATILE_DATA_REGION_NAME;

            for (CountDownLatchView l : latches0) {
                if ("c1".equals(l.name())) {
                    assertEquals(3, l.count());
                    assertEquals(3, l.initialCount());
                    assertFalse(l.autoDelete());

                    l1.countDown();

                    assertEquals(2, l.count());
                    assertEquals(3, l.initialCount());
                    assertFalse(l.removed());
                }
                else {
                    assertEquals("c2", l.name());
                    assertEquals(1, l.count());
                    assertEquals(1, l.initialCount());
                    assertTrue(l.autoDelete());
                    assertFalse(l.removed());

                    l2.countDown();
                    l2.close();

                    assertTrue(waitForCondition(l::removed, getTestTimeout()));
                }

                assertEquals(grpName, l.groupName());
                assertEquals(CU.cacheId(grpName), l.groupId());
            }

            IgniteCountDownLatch l3 = g1.countDownLatch("c1", 10, true, true);

            assertEquals(1, latches1.size());

            CountDownLatchView l = latches1.iterator().next();

            assertEquals(2, l.count());
            assertEquals(3, l.initialCount());
            assertEquals(grpName, l.groupName());
            assertEquals(CU.cacheId(grpName), l.groupId());
            assertFalse(l.removed());
            assertFalse(l.autoDelete());

            l3.countDown();
            l3.countDown();
            l3.close();

            assertTrue(waitForCondition(l::removed, getTestTimeout()));

            assertEquals(0, latches0.size());
            assertEquals(0, latches1.size());
        }
    }

    /** */
    @Test
    public void testSemaphores() throws Exception {
        try (IgniteEx g0 = startGrid(0);
             IgniteEx g1 = startGrid(1)) {
            IgniteSemaphore s1 = g0.semaphore("s1", 3, false, true);
            IgniteSemaphore s2 = g0.semaphore("s2", 1, true, true);

            SystemView<SemaphoreView> semaphores0 = g0.context().systemView().view(SEMAPHORES_VIEW);
            SystemView<SemaphoreView> semaphores1 = g1.context().systemView().view(SEMAPHORES_VIEW);

            assertEquals(2, semaphores0.size());
            assertEquals(0, semaphores1.size());

            String grpName = DEFAULT_VOLATILE_DS_GROUP_NAME + "@" + VOLATILE_DATA_REGION_NAME;

            IgniteInternalFuture<?> acquirePermitFut = null;

            for (SemaphoreView s : semaphores0) {
                if ("s1".equals(s.name())) {
                    assertEquals(3, s.availablePermits());
                    assertFalse(s.hasQueuedThreads());
                    assertEquals(0, s.queueLength());
                    assertFalse(s.failoverSafe());
                    assertFalse(s.broken());
                    assertFalse(s.removed());

                    acquirePermitFut = runAsync(() -> {
                        s1.acquire(2);

                        try {
                            Thread.sleep(getTestTimeout());
                        }
                        catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        finally {
                            s1.release(2);
                        }
                    });

                    assertTrue(waitForCondition(() -> s.availablePermits() == 1, getTestTimeout()));
                    assertTrue(s.hasQueuedThreads());
                    assertEquals(1, s.queueLength());
                }
                else {
                    assertEquals(1, s.availablePermits());
                    assertFalse(s.hasQueuedThreads());
                    assertEquals(0, s.queueLength());
                    assertTrue(s.failoverSafe());
                    assertFalse(s.broken());
                    assertFalse(s.removed());

                    s2.close();

                    assertTrue(waitForCondition(s::removed, getTestTimeout()));
                }

                assertEquals(grpName, s.groupName());
                assertEquals(CU.cacheId(grpName), s.groupId());
            }

            IgniteSemaphore l3 = g1.semaphore("s1", 10, true, true);

            assertEquals(1, semaphores1.size());

            SemaphoreView s = semaphores1.iterator().next();

            assertEquals(1, s.availablePermits());
            assertTrue(s.hasQueuedThreads());
            assertEquals(1, s.queueLength());
            assertFalse(s.failoverSafe());
            assertFalse(s.broken());
            assertFalse(s.removed());

            acquirePermitFut.cancel();
            assertTrue(waitForCondition(() -> s.availablePermits() == 3, getTestTimeout()));

            l3.close();

            assertTrue(waitForCondition(s::removed, getTestTimeout()));

            assertEquals(0, semaphores0.size());
            assertEquals(0, semaphores1.size());
        }
    }

    /** */
    @Test
    public void testLocks() throws Exception {
        try (IgniteEx g0 = startGrid(0);
             IgniteEx g1 = startGrid(1)) {
            IgniteLock l1 = g0.reentrantLock("l1", false, true, true);
            IgniteLock l2 = g0.reentrantLock("l2", true, false, true);

            SystemView<ReentrantLockView> locks0 = g0.context().systemView().view(LOCKS_VIEW);
            SystemView<ReentrantLockView> locks1 = g1.context().systemView().view(LOCKS_VIEW);

            assertEquals(2, locks0.size());
            assertEquals(0, locks1.size());

            String grpName = DEFAULT_VOLATILE_DS_GROUP_NAME + "@" + VOLATILE_DATA_REGION_NAME;

            IgniteInternalFuture<?> lockFut = null;

            Runnable lockNSleep = () -> {
                l1.lock();

                try {
                    Thread.sleep(getTestTimeout());
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                finally {
                    l1.unlock();
                }
            };

            for (ReentrantLockView l : locks0) {
                if ("l1".equals(l.name())) {
                    assertFalse(l.locked());
                    assertFalse(l.hasQueuedThreads());
                    assertFalse(l.failoverSafe());
                    assertTrue(l.fair());
                    assertFalse(l.broken());
                    assertFalse(l.removed());

                    lockFut = runAsync(lockNSleep);

                    assertTrue(waitForCondition(l::locked, getTestTimeout()));
                }
                else {
                    assertFalse(l.hasQueuedThreads());
                    assertTrue(l.failoverSafe());
                    assertFalse(l.fair());
                    assertFalse(l.broken());
                    assertFalse(l.removed());

                    l2.close();

                    assertTrue(waitForCondition(l::removed, getTestTimeout()));
                }

                assertEquals(grpName, l.groupName());
                assertEquals(CU.cacheId(grpName), l.groupId());
            }

            IgniteLock l3 = g1.reentrantLock("l1", true, false, true);

            assertEquals(1, locks1.size());

            ReentrantLockView s = locks1.iterator().next();

            assertTrue(s.locked());
            assertFalse(s.hasQueuedThreads());
            assertFalse(s.failoverSafe());
            assertTrue(s.fair());
            assertFalse(s.broken());
            assertFalse(s.removed());

            lockFut.cancel();

            assertTrue(waitForCondition(() -> !s.locked(), getTestTimeout()));
            assertFalse(s.hasQueuedThreads());

            l3.close();

            assertTrue(waitForCondition(s::removed, getTestTimeout()));

            assertEquals(0, locks0.size());
            assertEquals(0, locks1.size());
        }
    }

    /** */
    @Test
    public void testQueue() throws Exception {
        try (IgniteEx g0 = startGrid(0);
             IgniteEx g1 = startGrid(1)) {

            IgniteQueue<String> q0 = g0.queue("queue-1", 42, new CollectionConfiguration()
                .setCollocated(true)
                .setBackups(1)
                .setGroupName("my-group"));
            IgniteQueue<?> q1 = g0.queue("queue-2", 0, new CollectionConfiguration());

            SystemView<QueueView> queues0 = g0.context().systemView().view(QUEUES_VIEW);
            SystemView<QueueView> queues1 = g1.context().systemView().view(QUEUES_VIEW);

            assertEquals(2, queues0.size());
            assertEquals(0, queues1.size());

            for (QueueView q : queues0) {
                if ("queue-1".equals(q.name())) {
                    assertNotNull(q.id());
                    assertEquals("queue-1", q.name());
                    assertEquals(42, q.capacity());
                    assertTrue(q.bounded());
                    assertTrue(q.collocated());
                    assertEquals("my-group", q.groupName());
                    assertEquals(CU.cacheId("my-group"), q.groupId());
                    assertFalse(q.removed());
                    assertEquals(0, q.size());

                    q0.add("first");

                    assertEquals(1, q.size());
                }
                else {
                    assertNotNull(q.id());
                    assertEquals("queue-2", q.name());
                    assertEquals(Integer.MAX_VALUE, q.capacity());
                    assertFalse(q.bounded());
                    assertFalse(q.collocated());
                    assertEquals(DEFAULT_DS_GROUP_NAME, q.groupName());
                    assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), q.groupId());
                    assertFalse(q.removed());
                    assertEquals(0, q.size());

                    q1.close();

                    assertTrue(waitForCondition(q::removed, getTestTimeout()));
                }
            }

            IgniteQueue<?> q2 = g1.queue("queue-1", 42, new CollectionConfiguration()
                .setCollocated(true)
                .setBackups(1)
                .setGroupName("my-group"));

            assertEquals(1, queues1.size());

            QueueView q = queues1.iterator().next();

            assertNotNull(q.id());
            assertEquals("queue-1", q.name());
            assertEquals(42, q.capacity());
            assertTrue(q.bounded());
            assertTrue(q.collocated());
            assertEquals("my-group", q.groupName());
            assertEquals(CU.cacheId("my-group"), q.groupId());
            assertFalse(q.removed());
            assertEquals(1, q.size());

            q2.close();

            assertTrue(waitForCondition(q::removed, getTestTimeout()));

            assertEquals(0, queues0.size());
            assertEquals(0, queues1.size());
        }
    }

    /** */
    @Test
    public void testSet() throws Exception {
        try (IgniteEx g0 = startGrid(0);
             IgniteEx g1 = startGrid(1)) {

            IgniteSet<String> s0 = g0.set("set-1", new CollectionConfiguration()
                .setCollocated(true)
                .setBackups(1)
                .setGroupName("my-group"));
            IgniteSet<?> s1 = g0.set("set-2", new CollectionConfiguration());

            SystemView<SetView> sets0 = g0.context().systemView().view(SETS_VIEW);
            SystemView<SetView> sets1 = g1.context().systemView().view(SETS_VIEW);

            assertEquals(2, sets0.size());
            assertEquals(0, sets1.size());

            for (SetView q : sets0) {
                if ("set-1".equals(q.name())) {
                    assertNotNull(q.id());
                    assertEquals("set-1", q.name());
                    assertTrue(q.collocated());
                    assertEquals("my-group", q.groupName());
                    assertEquals(CU.cacheId("my-group"), q.groupId());
                    assertFalse(q.removed());
                    assertEquals(0, q.size());

                    s0.add("first");

                    assertEquals(1, q.size());
                }
                else {
                    assertNotNull(q.id());
                    assertEquals("set-2", q.name());
                    assertFalse(q.collocated());
                    assertEquals(DEFAULT_DS_GROUP_NAME, q.groupName());
                    assertEquals(CU.cacheId(DEFAULT_DS_GROUP_NAME), q.groupId());
                    assertFalse(q.removed());
                    assertEquals(0, q.size());

                    s1.close();

                    assertTrue(waitForCondition(q::removed, getTestTimeout()));
                }
            }

            IgniteSet<?> s2 = g1.set("set-1", new CollectionConfiguration()
                .setCollocated(true)
                .setBackups(1)
                .setGroupName("my-group"));

            assertEquals(1, sets1.size());

            SetView s = sets1.iterator().next();

            assertNotNull(s.id());
            assertEquals("set-1", s.name());
            assertTrue(s.collocated());
            assertEquals("my-group", s.groupName());
            assertEquals(CU.cacheId("my-group"), s.groupId());
            assertFalse(s.removed());
            assertEquals(1, s.size());

            s2.close();

            assertTrue(waitForCondition(s::removed, getTestTimeout()));

            assertEquals(0, sets0.size());
            assertEquals(0, sets1.size());
        }
    }

    /** */
    @Test
    public void testStripedExecutors() throws Exception {
        try (IgniteEx g = startGrid(0)) {
            checkStripeExecutorView(g.context().pools().getStripedExecutorService(),
                g.context().systemView().view(SYS_POOL_QUEUE_VIEW),
                "sys");

            checkStripeExecutorView(g.context().pools().getDataStreamerExecutorService(),
                g.context().systemView().view(STREAM_POOL_QUEUE_VIEW),
                "data-streamer");
        }
    }

    /**
     * Checks striped executor system view.
     *
     * @param execSvc Striped executor.
     * @param view System view.
     * @param poolName Executor name.
     */
    private void checkStripeExecutorView(StripedExecutor execSvc, SystemView<StripedExecutorTaskView> view,
        String poolName) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        execSvc.execute(0, new TestRunnable(latch, 0));
        execSvc.execute(0, new TestRunnable(latch, 1));
        execSvc.execute(1, new TestRunnable(latch, 2));
        execSvc.execute(1, new TestRunnable(latch, 3));

        try {
            boolean res = waitForCondition(() -> view.size() == 2, 5_000);

            assertTrue(res);

            Iterator<StripedExecutorTaskView> iter = view.iterator();

            assertTrue(iter.hasNext());

            StripedExecutorTaskView row0 = iter.next();

            assertEquals(0, row0.stripeIndex());
            assertEquals(TestRunnable.class.getSimpleName() + '1', row0.description());
            assertEquals(poolName + "-stripe-0", row0.threadName());
            assertEquals(TestRunnable.class.getName(), row0.taskName());

            assertTrue(iter.hasNext());

            StripedExecutorTaskView row1 = iter.next();

            assertEquals(1, row1.stripeIndex());
            assertEquals(TestRunnable.class.getSimpleName() + '3', row1.description());
            assertEquals(poolName + "-stripe-1", row1.threadName());
            assertEquals(TestRunnable.class.getName(), row1.taskName());
        }
        finally {
            latch.countDown();
        }
    }

    /** */
    public static class TestPredicate implements IgniteBiPredicate<Integer, Integer> {
        /** {@inheritDoc} */
        @Override public boolean apply(Integer integer, Integer integer2) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return TEST_PREDICATE;
        }
    }

    /** */
    public static class TestTransformer implements IgniteClosure<Cache.Entry<Integer, Integer>, Integer> {
        /** {@inheritDoc} */
        @Override public Integer apply(Cache.Entry<Integer, Integer> entry) {
            return entry.getKey();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return TEST_TRANSFORMER;
        }
    }

    /** */
    @Test
    public void testPagesList() throws Exception {
        cleanPersistenceDir();

        try (IgniteEx ignite = startGrid(getConfiguration()
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDataRegionConfigurations(
                    new DataRegionConfiguration().setName("dr0").setMaxSize(100L * 1024 * 1024),
                    new DataRegionConfiguration().setName("dr1").setMaxSize(100L * 1024 * 1024)
                        .setPersistenceEnabled(true)
                )))) {
            ignite.cluster().active(true);

            GridCacheDatabaseSharedManager dbMgr = (GridCacheDatabaseSharedManager)ignite.context().cache().context()
                .database();

            int pageSize = dbMgr.pageSize();

            dbMgr.enableCheckpoints(false).get();

            for (int i = 0; i < 2; i++) {
                IgniteCache<Object, Object> cache = ignite.getOrCreateCache(new CacheConfiguration<>("cache" + i)
                    .setDataRegionName("dr" + i).setAffinity(new RendezvousAffinityFunction().setPartitions(2)));

                int key = 0;

                // Fill up different free-list buckets.
                for (int j = 0; j < pageSize / 2; j++)
                    cache.put(key++, new byte[j + 1]);

                // Put some pages to one bucket to overflow pages cache.
                for (int j = 0; j < 1000; j++)
                    cache.put(key++, new byte[pageSize / 2]);
            }

            long dr0flPages = 0;
            int dr0flStripes = 0;

            SystemView<PagesListView> dataRegionPageLists = ignite.context().systemView().view(DATA_REGION_PAGE_LIST_VIEW);

            for (PagesListView pagesListView : dataRegionPageLists) {
                if (pagesListView.name().startsWith("dr0")) {
                    dr0flPages += pagesListView.bucketSize();
                    dr0flStripes += pagesListView.stripesCount();
                }
            }

            assertTrue(dr0flPages > 0);
            assertTrue(dr0flStripes > 0);

            SystemView<CachePagesListView> cacheGrpPageLists = ignite.context().systemView().view(CACHE_GRP_PAGE_LIST_VIEW);

            long dr1flPages = 0;
            int dr1flStripes = 0;
            int dr1flCached = 0;

            for (CachePagesListView pagesListView : cacheGrpPageLists) {
                if (pagesListView.cacheGroupId() == cacheId("cache1")) {
                    dr1flPages += pagesListView.bucketSize();
                    dr1flStripes += pagesListView.stripesCount();
                    dr1flCached += pagesListView.cachedPagesCount();
                }
            }

            assertTrue(dr1flPages > 0);
            assertTrue(dr1flStripes > 0);
            assertTrue(dr1flCached > 0);

            // Test filtering.
            assertTrue(cacheGrpPageLists instanceof FiltrableSystemView);

            Iterator<CachePagesListView> iter = ((FiltrableSystemView<CachePagesListView>)cacheGrpPageLists).iterator(U.map(
                CachePagesListViewWalker.CACHE_GROUP_ID_FILTER, cacheId("cache1"),
                CachePagesListViewWalker.PARTITION_ID_FILTER, 0,
                CachePagesListViewWalker.BUCKET_NUMBER_FILTER, 0
            ));

            assertEquals(1, F.size(iter));

            iter = ((FiltrableSystemView<CachePagesListView>)cacheGrpPageLists).iterator(U.map(
                CachePagesListViewWalker.CACHE_GROUP_ID_FILTER, cacheId("cache1"),
                CachePagesListViewWalker.BUCKET_NUMBER_FILTER, 0
            ));

            assertEquals(2, F.size(iter));
        }
    }

    /** */
    @Test
    public void testBinaryMeta() throws Exception {
        try (IgniteEx g = startGrid(0)) {
            IgniteCache<Integer, TestObjectAllTypes> c1 = g.createCache("test-cache");
            IgniteCache<Integer, TestObjectEnum> c2 = g.createCache("test-enum-cache");

            c1.put(1, new TestObjectAllTypes());
            c2.put(1, TestObjectEnum.A);

            SystemView<BinaryMetadataView> view = g.context().systemView().view(BINARY_METADATA_VIEW);

            assertNotNull(view);
            assertEquals(2, view.size());

            for (BinaryMetadataView meta : view) {
                if (TestObjectEnum.class.getName().contains(meta.typeName())) {
                    assertTrue(meta.isEnum());

                    assertEquals(0, meta.fieldsCount());
                }
                else {
                    assertFalse(meta.isEnum());

                    Field[] fields = TestObjectAllTypes.class.getDeclaredFields();

                    assertEquals(fields.length, meta.fieldsCount());

                    for (Field field : fields)
                        assertTrue(meta.fields().contains(field.getName()));
                }
            }
        }
    }

    /** */
    @Test
    public void testMetastorage() throws Exception {
        cleanPersistenceDir();

        try (IgniteEx ignite = startGrid(getConfiguration().setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            )))) {
            ignite.cluster().state(ClusterState.ACTIVE);

            IgniteCacheDatabaseSharedManager db = ignite.context().cache().context().database();

            SystemView<MetastorageView> metaStoreView = ignite.context().systemView().view(METASTORE_VIEW);

            assertNotNull(metaStoreView);

            String name = "test-key";
            String val = "test-value";
            String unmarshalledName = "unmarshalled-key";
            String unmarshalledVal = "[Raw data. 0 bytes]";

            db.checkpointReadLock();

            try {
                db.metaStorage().write(name, val);
                db.metaStorage().writeRaw(unmarshalledName, new byte[0]);
            } finally {
                db.checkpointReadUnlock();
            }

            assertNotNull(F.find(metaStoreView, null,
                (IgnitePredicate<? super MetastorageView>)view ->
                    name.equals(view.name()) && val.equals(view.value())));

            assertNotNull(F.find(metaStoreView, null,
                (IgnitePredicate<? super MetastorageView>)view ->
                    unmarshalledName.equals(view.name()) && unmarshalledVal.equals(view.value())));
        }
    }

    /** */
    @Test
    public void testDistributedMetastorage() throws Exception {
        cleanPersistenceDir();

        try (IgniteEx ignite = startGrid(getConfiguration().setDataStorageConfiguration(
            new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                new DataRegionConfiguration().setPersistenceEnabled(true)
            )))) {
            ignite.cluster().state(ClusterState.ACTIVE);

            DistributedMetaStorage dms = ignite.context().distributedMetastorage();

            SystemView<MetastorageView> metaStoreView = ignite.context().systemView().view(DISTRIBUTED_METASTORE_VIEW);

            assertNotNull(metaStoreView);

            String name = "test-distributed-key";
            String val = "test-distributed-value";

            dms.write(name, val);

            MetastorageView testKey = F.find(metaStoreView, null,
                (IgnitePredicate<? super MetastorageView>)view -> name.equals(view.name()) && val.equals(view.value()));

            assertNotNull(testKey);
        }
    }

    /** */
    @Test
    public void testBaselineNodes() throws Exception {
        cleanPersistenceDir();

        try (
            IgniteEx ignite0 = startGrid(getConfiguration(getTestIgniteInstanceName(0))
                .setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)))
                .setConsistentId("consId0"));
            IgniteEx ignite1 = startGrid(getConfiguration(getTestIgniteInstanceName(1))
                .setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)))
                .setConsistentId("consId1"));
        ) {
            ignite0.cluster().state(ClusterState.ACTIVE);

            ignite1.close();

            awaitPartitionMapExchange();

            SystemView<BaselineNodeView> view = ignite0.context().systemView().view(BASELINE_NODES_SYS_VIEW);

            assertEquals(2, view.size());
            assertEquals(1, F.size(view.iterator(), row -> "consId0".equals(row.consistentId()) && row.online()));
            assertEquals(1, F.size(view.iterator(), row -> "consId1".equals(row.consistentId()) && !row.online()));
        }
    }

    /** */
    @Test
    public void testBaselineNodeAttributes() throws Exception {
        cleanPersistenceDir();

        try (IgniteEx ignite = startGrid(getConfiguration()
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setName("pds").setPersistenceEnabled(true)
                ))
            .setUserAttributes(F.asMap("name", "val"))
            .setConsistentId("consId"))
        ) {
            ignite.cluster().state(ClusterState.ACTIVE);

            SystemView<BaselineNodeAttributeView> view = ignite.context().systemView()
                .view(BASELINE_NODE_ATTRIBUTES_SYS_VIEW);

            assertEquals(ignite.cluster().localNode().attributes().size(), view.size());

            assertEquals(1, F.size(view.iterator(), row -> "consId".equals(row.nodeConsistentId()) &&
                "name".equals(row.name()) && "val".equals(row.value())));

            // Test filtering.
            assertTrue(view instanceof FiltrableSystemView);

            Iterator<BaselineNodeAttributeView> iter = ((FiltrableSystemView<BaselineNodeAttributeView>)view)
                .iterator(F.asMap(BaselineNodeAttributeViewWalker.NODE_CONSISTENT_ID_FILTER, "consId",
                    BaselineNodeAttributeViewWalker.NAME_FILTER, "name"));

            assertEquals(1, F.size(iter));

            iter = ((FiltrableSystemView<BaselineNodeAttributeView>)view).iterator(
                F.asMap(BaselineNodeAttributeViewWalker.NODE_CONSISTENT_ID_FILTER, "consId"));

            assertEquals(1, F.size(iter, row -> "name".equals(row.name())));

            iter = ((FiltrableSystemView<BaselineNodeAttributeView>)view).iterator(
                F.asMap(BaselineNodeAttributeViewWalker.NAME_FILTER, "name"));

            assertEquals(1, F.size(iter));
        }
    }

    /** */
    @Test
    public void testSnapshot() throws Exception {
        cleanPersistenceDir();

        String dfltCacheGrp = "testGroup";

        String testSnap0 = "testSnap0";
        String testSnap1 = "testSnap1";

        try (IgniteEx ignite = startGrid(getConfiguration()
            .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME).setGroupName(dfltCacheGrp))
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setName("pds").setPersistenceEnabled(true)
                )))
        ) {
            ignite.cluster().state(ClusterState.ACTIVE);

            ignite.cache(DEFAULT_CACHE_NAME).put(1, 1);

            ignite.snapshot().createSnapshot(testSnap0).get();

            SystemView<SnapshotView> views = ignite.context().systemView().view(SNAPSHOT_SYS_VIEW);

            assertEquals(1, views.size());

            SnapshotView view = F.first(views);

            assertEquals(testSnap0, view.name());
            assertEquals(ignite.localNode().consistentId().toString(), view.consistentId());

            Collection<?> constIds = F.nodeConsistentIds(ignite.cluster().nodes());

            assertEquals(F.concat(constIds, ","), view.baselineNodes());
            assertEquals(F.concat(Arrays.asList(dfltCacheGrp, METASTORAGE_CACHE_NAME), ","), view.cacheGroups());

            ignite.createCache("testCache");

            ignite.snapshot().createSnapshot(testSnap1).get();

            views = ignite.context().systemView().view(SNAPSHOT_SYS_VIEW);

            assertEquals(2, views.size());

            List<String> exp = Lists.newArrayList(testSnap0, testSnap1);

            views.forEach(v -> assertTrue(exp.remove(v.name())));

            assertTrue(exp.isEmpty());
        }
    }

    /** Test node filter. */
    public static class TestNodeFilter implements IgnitePredicate<ClusterNode> {
        /** {@inheritDoc} */
        @Override public boolean apply(ClusterNode node) {
            return true;
        }
    }

    /** Test runnable. */
    public static class TestRunnable implements Runnable {
        /** */
        private final CountDownLatch latch;

        /** */
        private final int idx;

        /** */
        public TestRunnable(CountDownLatch latch, int idx) {
            this.latch = latch;
            this.idx = idx;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            try {
                latch.await(5, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return getClass().getSimpleName() + idx;
        }
    }
}
