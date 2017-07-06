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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheEntryEventSerializableFilter;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.CacheQueryEntryEvent;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.continuous.GridContinuousMessage;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.internal.util.typedef.PAX;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteAsyncCallback;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.spi.eventstorage.memory.MemoryEventStorageSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class CacheContinuousQueryIsPrimaryFlagTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static volatile boolean err;

    /** */
    private CacheConfiguration<Object, Object> ccfg;

    /** */
    private boolean client;

    /** */
    private int backups = 2;

    /** */
    private boolean async = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setLateAffinityAssignment(true);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        TestCommunicationSpi commSpi = new TestCommunicationSpi();

        commSpi.setSharedMemoryPort(-1);

        cfg.setCommunicationSpi(commSpi);

        MemoryEventStorageSpi evtSpi = new MemoryEventStorageSpi();
        evtSpi.setExpireCount(50);

        cfg.setEventStorageSpi(evtSpi);

        cfg.setCacheConfiguration(ccfg);

        cfg.setClientMode(client);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60_000;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        err = false;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @param cacheMode Cache mode.
     * @param backups Number of backups.
     * @param atomicityMode Cache atomicity mode.
     * @return Cache configuration.
     */
    protected CacheConfiguration<Object, Object> cacheConfiguration(
        CacheMode cacheMode,
        int backups,
        CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Object, Object> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(backups);
        ccfg.setCacheMode(cacheMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReceiveFlagForBackupNodePartitionedAtomic() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, backups, ATOMIC);

        leftPrimaryNode();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReceiveFlagForBackupNodePartitionedTx() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, backups, TRANSACTIONAL);

        leftPrimaryNode();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReceiveFlagForBackupNodeReplicatedAtomic() throws Exception {
        ccfg = cacheConfiguration(REPLICATED, backups, ATOMIC);

        leftPrimaryNode();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReceiveFlagForBackupNodeReplicatedTx() throws Exception {
        ccfg = cacheConfiguration(REPLICATED, backups, TRANSACTIONAL);

        leftPrimaryNode();
    }

    /**
     *
     */
    public void leftPrimaryNode() throws Exception {
        final int SRV_NODES = 3;

        startGridsMultiThreaded(SRV_NODES);

        client = true;

        final Ignite qryClient = startGrid(SRV_NODES);

        client = false;

        IgniteCache<Object, Object> qryClientCache = qryClient.cache(DEFAULT_CACHE_NAME);

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        final CacheEventListener lsnr = new CacheEventListener();

        qry.setLocalListener(lsnr);

        QueryCursor<?> cur = qryClientCache.query(qry);

        Object key = 1;

        Ignite primary = primaryNode(key, qryClientCache.getName());

        TestCommunicationSpi spi = (TestCommunicationSpi)primary.configuration().getCommunicationSpi();

        spi.primaryNode = primary.cluster().localNode();

        spi.clientNode = qryClient.cluster().localNode();

        spi.skipFirstMsg = new AtomicBoolean(true);

        qryClientCache.put(key, key);

        U.await(spi.latch, 5000L, TimeUnit.MILLISECONDS);

        stopGrid(primary.name(), true);

        awaitPartitionMapExchange();

        assert GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return qryClient.cluster().nodes().size() == (SRV_NODES + 1 /** client node */)
                    - 1 /** Primary node */;
            }
        }, 5000L);

        assertTrue(lsnr.isBackup.get());

        cur.close();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReceiveFlagsForAllNodesPartitionedAtomic() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, backups, ATOMIC);

        receivePrimaryAndBackupFlags();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReceiveFlagsForAllNodesPartitionedTx() throws Exception {
        ccfg = cacheConfiguration(PARTITIONED, backups, TRANSACTIONAL);

        receivePrimaryAndBackupFlags();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReceiveFlagsForAllNodesReplicatedAtomic() throws Exception {
        ccfg = cacheConfiguration(REPLICATED, backups, ATOMIC);

        receivePrimaryAndBackupFlags();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReceiveFlagsForAllNodesReplicatedTx() throws Exception {
        ccfg = cacheConfiguration(REPLICATED, backups, TRANSACTIONAL);

        receivePrimaryAndBackupFlags();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReceiveFlagsForAllNodesPartitionedAtomicAsync() throws Exception {
        async = true;
        ccfg = cacheConfiguration(PARTITIONED, backups, ATOMIC);

        receivePrimaryAndBackupFlags();
    }

    /**
     * @throws Exception If failed.
     */
    public void receivePrimaryAndBackupFlags() throws Exception {
        final int SRV_NODES = 5;

        startGridsMultiThreaded(SRV_NODES);

        client = true;

        Ignite qryClient = startGrid(SRV_NODES);

        client = false;

        IgniteCache<Object, Object> qryClientCache = qryClient.cache(DEFAULT_CACHE_NAME);

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        final CacheEventListener lsnr = asyncCallback() ? new CacheEventAsyncListener() : new CacheEventListener();

        qry.setLocalListener(lsnr);

        qry.setRemoteFilter(lsnr);

        QueryCursor<?> cur = qryClientCache.query(qry);

        Ignite ignite = ignite(0);

        final int SIZE = 500;

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        Affinity<Object> aff = qryClient.affinity(DEFAULT_CACHE_NAME);

        List<Integer> keys = new ArrayList<>();

        for (int key = 0; key < SIZE; key++)
            keys.add(key);

        final List<T2<Object, Object>> expEvts = new ArrayList<>();

        for (Integer key : keys) {
            log.info("Put [node=" + ignite.name() + ", key=" + key + ", part=" + aff.partition(key) + ']');

            Integer val = key * 2;

            expEvts.add(new T2<>((Object)key, (Object)val));

            cache.put(key, val);
        }

        boolean check = GridTestUtils.waitForCondition(new PAX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                return expEvts.size() == lsnr.evts.size();
            }
        }, 5000L);

        if (!check) {
            Set<Integer> keys0 = new HashSet<>(keys);

            keys0.removeAll(lsnr.evts.keySet());

            log.info("Missed events for keys: " + keys0);

            fail("Failed to wait for notifications [exp=" + keys.size() + ", left=" + keys0.size() + ']');
        }

        final CacheConfiguration ccfg = qryClientCache.getConfiguration(CacheConfiguration.class);

        if (ccfg.getCacheMode() == REPLICATED)
            assertEquals(keys.size() * SRV_NODES, lsnr.evtsFlags.size());
        else
            assertEquals(keys.size() * (this.backups + 1 /** primary node */), lsnr.evtsFlags.size());

        checkFlags(keys.size(), lsnr.evtsFlags, SRV_NODES, ccfg);

        expEvts.clear();
        lsnr.evtsFlags.clear();
        lsnr.evts.clear();

        cur.close();

        stopAllGrids();
    }

    /**
     *
     */
    private boolean asyncCallback() {
        return async;
    }

    /**
     *
     */
    private void checkFlags(int size, GridConcurrentHashSet<CacheQueryEntryEvent> evtsFlags, int nodes, CacheConfiguration ccfg) {
        int backup = 0;
        int primary = 0;

        for (CacheQueryEntryEvent evt : evtsFlags) {
            if (evt.isPrimary())
                primary++;
            else if (evt.isBackup())
                backup++;
        }

        assertEquals(size, primary);

        if (ccfg.getCacheMode() == REPLICATED)
            assertEquals(size * (nodes - 1 /** primary node */), backup);
        else
            assertEquals(size * this.backups, backup);
    }

    /**
     *
     */
    @IgniteAsyncCallback
    public static class CacheEventAsyncListener extends CacheEventListener {
        // No-op.
    }

    /**
     *
     */
    public static class CacheEventListener implements CacheEntryUpdatedListener<Object, Object>,
        CacheEntryEventSerializableFilter<Object, Object> {
        /** Events. */
        private final ConcurrentHashMap<Object, CacheEntryEvent<?, ?>> evts = new ConcurrentHashMap<>();

        /** Filtred events. */
        private final static GridConcurrentHashSet<CacheQueryEntryEvent> evtsFlags = new GridConcurrentHashSet<>();

        /** Flag backups node. */
        private volatile static AtomicBoolean isBackup = new AtomicBoolean(false);

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<?, ?> e) throws CacheEntryListenerException {
            evtsFlags.add(e.unwrap(CacheQueryEntryEvent.class));

            return true;
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) throws CacheEntryListenerException {
            for (CacheEntryEvent<?, ?> e : evts) {
                Integer key = (Integer)e.getKey();

                isBackup.set(e.unwrap(CacheQueryEntryEvent.class).isBackup());

                assertNull(this.evts.put(key, e));
            }
        }
    }

    /**
     *
     */
    private static class TestCommunicationSpi extends TcpCommunicationSpi {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private volatile static AtomicBoolean skipFirstMsg;

        /** */
        private volatile static ClusterNode clientNode = null;

        /** */
        private volatile static ClusterNode primaryNode = null;

        /** */
        private final static CountDownLatch latch = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            Object msg0 = ((GridIoMessage)msg).message();

            if (msg0 instanceof GridContinuousMessage) {
                if (clientNode != null && primaryNode != null && skipFirstMsg != null &&
                    node.id().equals(clientNode.id()) &&
                    this.getLocalNode().id().equals(primaryNode.id()) &&
                    skipFirstMsg.compareAndSet(true, false)) {
                    assertEquals(node.id(), clientNode.id());

                    if (log.isDebugEnabled())
                        log.debug("Skip continuous message: " + msg0);

                    latch.countDown();

                    return;
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
