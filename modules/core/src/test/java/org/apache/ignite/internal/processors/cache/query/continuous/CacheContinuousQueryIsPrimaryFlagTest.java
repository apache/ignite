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
    private CacheMode cacheMode;

    /** */
    private CacheAtomicityMode atomicityMode;

    /** */
    private boolean client;

    /** */
    private int backups = 1;

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

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setBackups(backups);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

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
     * @throws Exception If failed.
     */
    public void testPartitionedAtomic() throws Exception {
        cacheMode = PARTITIONED;
        atomicityMode = ATOMIC;

        leftPrimaryNode();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPartitionedTx() throws Exception {
        cacheMode = PARTITIONED;
        atomicityMode = TRANSACTIONAL;

        leftPrimaryNode();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedAtomic() throws Exception {
        cacheMode = REPLICATED;
        atomicityMode = ATOMIC;

        leftPrimaryNode();
    }

    /**
     * @throws Exception If failed.
     */
    public void testReplicatedTx() throws Exception {
        cacheMode = REPLICATED;
        atomicityMode = TRANSACTIONAL;

        leftPrimaryNode();
    }

    /**
     *
     */
    public void leftPrimaryNode() throws Exception {
        this.backups = 2;

        final int SRV_NODES = 3;

        startGridsMultiThreaded(SRV_NODES);

        client = true;

        final Ignite qryClient = startGrid(SRV_NODES);

        client = false;

        IgniteCache<Object, Object> qryClientCache = qryClient.cache(DEFAULT_CACHE_NAME);

        if (cacheMode != REPLICATED)
            assertEquals(backups, qryClientCache.getConfiguration(CacheConfiguration.class).getBackups());

        assertNull(qryClientCache.getConfiguration(CacheConfiguration.class).getNearConfiguration());

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
    public void testPrimaryAndBackupFlags() throws Exception {
        this.cacheMode = PARTITIONED;

        this.atomicityMode = ATOMIC;

        this.backups = 2;

        final int SRV_NODES = 4;

        startGridsMultiThreaded(SRV_NODES);

        client = true;

        Ignite qryClient = startGrid(SRV_NODES);

        client = false;

        IgniteCache<Object, Object> qryClientCache = qryClient.cache(DEFAULT_CACHE_NAME);

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        final CacheEventListener2 lsnr = new CacheEventListener2();

        qry.setLocalListener(lsnr);

        qry.setRemoteFilter(lsnr);

        QueryCursor<?> cur = qryClientCache.query(qry);

        Ignite ignite = ignite(0);

        final int SIZE = 500;

        IgniteCache<Object, Object> cache = ignite.cache(DEFAULT_CACHE_NAME);

        Affinity<Object> aff = qryClient.affinity(DEFAULT_CACHE_NAME);

        List<Integer> keys = testKeys(SIZE);

        final List<T2<Object, Object>> expEvts = new ArrayList<>();

        for (Integer key : keys) {
            log.info("Put [node=" + ignite.name() + ", key=" + key + ", part=" + aff.partition(key) + ']');

            Integer val = key * 2;

            expEvts.add(new T2<>((Object)key, (Object)val));

            cache.put(key, val);
        }

        boolean check = GridTestUtils.waitForCondition(new PAX() {
            @Override public boolean applyx() throws IgniteCheckedException {
                return expEvts.size() == lsnr.keys.size();
            }
        }, 5000L);

        if (!check) {
            Set<Integer> keys0 = new HashSet<>(keys);

            keys0.removeAll(lsnr.keys);

            log.info("Missed events for keys: " + keys0);

            fail("Failed to wait for notifications [exp=" + keys.size() + ", left=" + keys0.size() + ']');
        }

        assertEquals(keys.size() * (SRV_NODES - 1 /** client node */), lsnr.evtsFlags.size());

        checkFlags(keys.size(), lsnr.evtsFlags);

        stopAllGrids();

        cur.close();
    }

    /**
     *
     */
    private void checkFlags(int size, GridConcurrentHashSet<CacheQueryEntryEvent> evtsFlags) {
        int backup = 0;
        int primary = 0;

        for (CacheQueryEntryEvent evt : evtsFlags) {
            if (evt.isPrimary())
                primary++;
            else if (evt.isBackup())
                backup++;
        }

        assertEquals(size, primary);
        assertEquals(size * this.backups, backup);
    }

    /**
     *
     */
    public static class CacheEventListener implements CacheEntryUpdatedListener<Object, Object> {
        /** Keys. */
        private final GridConcurrentHashSet<Integer> keys = new GridConcurrentHashSet<>();

        /** Events. */
        private final ConcurrentHashMap<Object, CacheEntryEvent<?, ?>> evts = new ConcurrentHashMap<>();

        /** */
        private volatile static AtomicBoolean isBackup = new AtomicBoolean(false);

        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) throws CacheEntryListenerException {
            for (CacheEntryEvent<?, ?> e : evts) {
                Integer key = (Integer)e.getKey();

                keys.add(key);

                isBackup.set(e.unwrap(CacheQueryEntryEvent.class).isBackup());

                assert this.evts.put(key, e) == null;
            }
        }
    }

    /**
     *
     */
    public static class CacheEventListener2 implements CacheEntryUpdatedListener<Object, Object>,
        CacheEntryEventSerializableFilter<Object, Object> {

        /** Keys. */
        private final GridConcurrentHashSet<Integer> keys = new GridConcurrentHashSet<>();

        /** Events. */
        private final ConcurrentHashMap<Object, CacheEntryEvent<?, ?>> evts = new ConcurrentHashMap<>();

        /** */
        private final static GridConcurrentHashSet<CacheQueryEntryEvent> evtsFlags = new GridConcurrentHashSet<>();

        /** {@inheritDoc} */
        @Override public boolean evaluate(CacheEntryEvent<?, ?> e) throws CacheEntryListenerException {
            evtsFlags.add(e.unwrap(CacheQueryEntryEvent.class));

            return true;
        }

        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) throws CacheEntryListenerException {
            for (CacheEntryEvent<?, ?> e : evts) {
                Integer key = (Integer)e.getKey();

                keys.add(key);

                assert this.evts.put(key, e) == null;
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

    /**
     *
     */
    private List<Integer> testKeys(int size) throws Exception {
        List<Integer> res = new ArrayList<>();

        for (int key = 0; key < size; key++) {
            res.add(key);
        }

        return res;
    }
}
