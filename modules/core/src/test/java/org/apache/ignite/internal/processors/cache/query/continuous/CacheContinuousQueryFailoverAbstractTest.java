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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.processors.continuous.GridContinuousHandler;
import org.apache.ignite.internal.processors.continuous.GridContinuousMessage;
import org.apache.ignite.internal.processors.continuous.GridContinuousProcessor;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.T3;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.CLOCK;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public abstract class CacheContinuousQueryFailoverAbstractTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int BACKUP_ACK_THRESHOLD = 100;

    /** */
    private static volatile boolean err;

    /** */
    private boolean client;

    /** */
    private int backups = 1;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setForceServerMode(true);
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        TestCommunicationSpi commSpi = new TestCommunicationSpi();

        commSpi.setIdleConnectionTimeout(100);

        cfg.setCommunicationSpi(commSpi);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(cacheMode());
        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setAtomicWriteOrderMode(writeOrderMode());
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
     * @return Cache mode.
     */
    protected abstract CacheMode cacheMode();

    /**
     * @return Atomicity mode.
     */
    protected abstract CacheAtomicityMode atomicityMode();

    /**
     * @return Write order mode for atomic cache.
     */
    protected CacheAtomicWriteOrderMode writeOrderMode() {
        return CLOCK;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRebalanceVersion() throws Exception {
        Ignite ignite0 = startGrid(0);
        GridDhtPartitionTopology top0 = ((IgniteKernal)ignite0).context().cache().context().cacheContext(1).topology();

        assertTrue(top0.rebalanceFinished(new AffinityTopologyVersion(1)));
        assertFalse(top0.rebalanceFinished(new AffinityTopologyVersion(2)));

        Ignite ignite1 = startGrid(1);
        GridDhtPartitionTopology top1 = ((IgniteKernal)ignite1).context().cache().context().cacheContext(1).topology();

        waitRebalanceFinished(ignite0, 2);
        waitRebalanceFinished(ignite1, 2);

        assertFalse(top0.rebalanceFinished(new AffinityTopologyVersion(3)));
        assertFalse(top1.rebalanceFinished(new AffinityTopologyVersion(3)));

        Ignite ignite2 = startGrid(2);
        GridDhtPartitionTopology top2 = ((IgniteKernal)ignite2).context().cache().context().cacheContext(1).topology();

        waitRebalanceFinished(ignite0, 3);
        waitRebalanceFinished(ignite1, 3);
        waitRebalanceFinished(ignite2, 3);

        assertFalse(top0.rebalanceFinished(new AffinityTopologyVersion(4)));
        assertFalse(top1.rebalanceFinished(new AffinityTopologyVersion(4)));
        assertFalse(top2.rebalanceFinished(new AffinityTopologyVersion(4)));

        client = true;

        Ignite ignite3 = startGrid(3);
        GridDhtPartitionTopology top3 = ((IgniteKernal)ignite3).context().cache().context().cacheContext(1).topology();

        assertTrue(top0.rebalanceFinished(new AffinityTopologyVersion(4)));
        assertTrue(top1.rebalanceFinished(new AffinityTopologyVersion(4)));
        assertTrue(top2.rebalanceFinished(new AffinityTopologyVersion(4)));
        assertTrue(top3.rebalanceFinished(new AffinityTopologyVersion(4)));

        stopGrid(1);

        waitRebalanceFinished(ignite0, 5);
        waitRebalanceFinished(ignite2, 5);
        waitRebalanceFinished(ignite3, 5);

        stopGrid(3);

        assertTrue(top0.rebalanceFinished(new AffinityTopologyVersion(6)));
        assertTrue(top2.rebalanceFinished(new AffinityTopologyVersion(6)));

        stopGrid(0);

        waitRebalanceFinished(ignite2, 7);
    }

    /**
     * @param ignite Ignite.
     * @param topVer Topology version.
     * @throws Exception If failed.
     */
    private void waitRebalanceFinished(Ignite ignite, long topVer) throws Exception {
        final AffinityTopologyVersion topVer0 = new AffinityTopologyVersion(topVer);

        final GridDhtPartitionTopology top =
            ((IgniteKernal)ignite).context().cache().context().cacheContext(1).topology();

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return top.rebalanceFinished(topVer0);
            }
        }, 5000);

        assertTrue(top.rebalanceFinished(topVer0));
    }

    /**
     * @throws Exception If failed.
     */
    public void testOneBackup() throws Exception {
        checkBackupQueue(1, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOneBackupClientUpdate() throws Exception {
        checkBackupQueue(1, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testThreeBackups() throws Exception {
        if (cacheMode() == REPLICATED)
            return;

        checkBackupQueue(3, false);
    }

    /**
     * @param backups Number of backups.
     * @param updateFromClient If {@code true} executes cache update from client node.
     * @throws Exception If failed.
     */
    private void checkBackupQueue(int backups, boolean updateFromClient) throws Exception {
        this.backups = backups;

        final int SRV_NODES = 4;

        startGridsMultiThreaded(SRV_NODES);

        client = true;

        Ignite qryClient = startGrid(SRV_NODES);

        client = false;

        IgniteCache<Object, Object> qryClientCache = qryClient.cache(null);

        if (cacheMode() != REPLICATED)
            assertEquals(backups, qryClientCache.getConfiguration(CacheConfiguration.class).getBackups());

        Affinity<Object> aff = qryClient.affinity(null);

        CacheEventListener1 lsnr = new CacheEventListener1(false);

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setLocalListener(lsnr);

        QueryCursor<?> cur = qryClientCache.query(qry);

        int PARTS = 10;

        Map<Object, T2<Object, Object>> updates = new HashMap<>();

        List<T3<Object, Object, Object>> expEvts = new ArrayList<>();

        for (int i = 0; i < SRV_NODES - 1; i++) {
            log.info("Stop iteration: " + i);

            TestCommunicationSpi spi = (TestCommunicationSpi)ignite(i).configuration().getCommunicationSpi();

            Ignite ignite = ignite(i);

            IgniteCache<Object, Object> cache = ignite.cache(null);

            List<Integer> keys = testKeys(cache, PARTS);

            CountDownLatch latch = new CountDownLatch(keys.size());

            lsnr.latch = latch;

            boolean first = true;

            for (Integer key : keys) {
                log.info("Put [node=" + ignite.name() + ", key=" + key + ", part=" + aff.partition(key) + ']');

                T2<Object, Object> t = updates.get(key);

                if (t == null) {
                    updates.put(key, new T2<>((Object)key, null));

                    expEvts.add(new T3<>((Object)key, (Object)key, null));
                }
                else {
                    updates.put(key, new T2<>((Object)key, (Object)key));

                    expEvts.add(new T3<>((Object)key, (Object)key, (Object)key));
                }

                if (updateFromClient)
                    qryClientCache.put(key, key);
                else
                    cache.put(key, key);

                if (first) {
                    spi.skipMsg = true;

                    first = false;
                }
            }

            stopGrid(i);

            if (!latch.await(5, SECONDS)) {
                Set<Integer> keys0 = new HashSet<>(keys);

                keys0.removeAll(lsnr.keys);

                log.info("Missed events for keys: " + keys0);

                fail("Failed to wait for notifications [exp=" + keys.size() + ", left=" + lsnr.latch.getCount() + ']');
            }

            checkEvents(expEvts, lsnr);
        }

        for (int i = 0; i < SRV_NODES - 1; i++) {
            log.info("Start iteration: " + i);

            Ignite ignite = startGrid(i);

            IgniteCache<Object, Object> cache = ignite.cache(null);

            List<Integer> keys = testKeys(cache, PARTS);

            CountDownLatch latch = new CountDownLatch(keys.size());

            lsnr.latch = latch;

            for (Integer key : keys) {
                log.info("Put [node=" + ignite.name() + ", key=" + key + ", part=" + aff.partition(key) + ']');

                T2<Object, Object> t = updates.get(key);

                if (t == null) {
                    updates.put(key, new T2<>((Object)key, null));

                    expEvts.add(new T3<>((Object)key, (Object)key, null));
                }
                else {
                    updates.put(key, new T2<>((Object)key, (Object)key));

                    expEvts.add(new T3<>((Object)key, (Object)key, (Object)key));
                }

                if (updateFromClient)
                    qryClientCache.put(key, key);
                else
                    cache.put(key, key);
            }

            if (!latch.await(5, SECONDS)) {
                Set<Integer> keys0 = new HashSet<>(keys);

                keys0.removeAll(lsnr.keys);

                log.info("Missed events for keys: " + keys0);

                fail("Failed to wait for notifications [exp=" + keys.size() + ", left=" + lsnr.latch.getCount() + ']');
            }

            checkEvents(expEvts, lsnr);
        }

        cur.close();

        assertFalse("Unexpected error during test, see log for details.", err);
    }

    /**
     * @param expEvts Expected events.
     * @param lsnr Listener.
     */
    private void checkEvents(List<T3<Object, Object, Object>> expEvts, CacheEventListener1 lsnr) {
        for (T3<Object, Object, Object> exp : expEvts) {
            CacheEntryEvent<?, ?> e = lsnr.evts.get(exp.get1());

            assertNotNull("No event for key: " + exp.get1(), e);
            assertEquals("Unexpected value: " + e, exp.get2(), e.getValue());
            assertEquals("Unexpected old value: " + e, exp.get3(), e.getOldValue());
        }

        expEvts.clear();

        lsnr.evts.clear();
    }

    /**
     * @param cache Cache.
     * @param parts Number of partitions.
     * @return Keys.
     */
    private List<Integer> testKeys(IgniteCache<Object, Object> cache, int parts) {
        Ignite ignite = cache.unwrap(Ignite.class);

        List<Integer> res = new ArrayList<>();

        Affinity<Object> aff = ignite.affinity(cache.getName());

        ClusterNode node = ignite.cluster().localNode();

        int[] nodeParts = aff.primaryPartitions(node);

        final int KEYS_PER_PART = 3;

        for (int i = 0; i < parts; i++) {
            int part = nodeParts[i];

            int cnt = 0;

            for (int key = 0; key < 100_000; key++) {
                if (aff.partition(key) == part && aff.isPrimary(node, key)) {
                    res.add(key);

                    if (++cnt == KEYS_PER_PART)
                        break;
                }
            }

            assertEquals(KEYS_PER_PART, cnt);
        }

        assertEquals(parts * KEYS_PER_PART, res.size());

        return res;
    }

    /**
     * @throws Exception If failed.
     */
    public void testBackupQueueCleanupClientQuery() throws Exception {
        startGridsMultiThreaded(2);

        client = true;

        Ignite qryClient = startGrid(2);

        CacheEventListener1 lsnr = new CacheEventListener1(false);

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setLocalListener(lsnr);

        QueryCursor<?> cur = qryClient.cache(null).query(qry);

        final Collection<Object> backupQueue = backupQueue(ignite(1));

        assertEquals(0, backupQueue.size());

        IgniteCache<Object, Object> cache0 = ignite(0).cache(null);

        List<Integer> keys = primaryKeys(cache0, BACKUP_ACK_THRESHOLD);

        CountDownLatch latch = new CountDownLatch(keys.size());

        lsnr.latch = latch;

        for (Integer key : keys) {
            log.info("Put: " + key);

            cache0.put(key, key);
        }

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return backupQueue.isEmpty();
            }
        }, 2000);

        assertTrue("Backup queue is not cleared: " + backupQueue, backupQueue.size() < BACKUP_ACK_THRESHOLD);

        if (!latch.await(5, SECONDS))
            fail("Failed to wait for notifications [exp=" + keys.size() + ", left=" + lsnr.latch.getCount() + ']');

        keys = primaryKeys(cache0, BACKUP_ACK_THRESHOLD / 2);

        latch = new CountDownLatch(keys.size());

        lsnr.latch = latch;

        for (Integer key : keys)
            cache0.put(key, key);

        final long ACK_FREQ = 5000;

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return backupQueue.isEmpty();
            }
        }, ACK_FREQ + 2000);

        assertTrue("Backup queue is not cleared: " + backupQueue, backupQueue.isEmpty());

        if (!latch.await(5, SECONDS))
            fail("Failed to wait for notifications [exp=" + keys.size() + ", left=" + lsnr.latch.getCount() + ']');

        cur.close();

        assertFalse("Unexpected error during test, see log for details.", err);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBackupQueueCleanupServerQuery() throws Exception {
        Ignite qryClient = startGridsMultiThreaded(2);

        CacheEventListener1 lsnr = new CacheEventListener1(false);

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setLocalListener(lsnr);

        IgniteCache<Object, Object> cache = qryClient.cache(null);

        QueryCursor<?> cur = cache.query(qry);

        final Collection<Object> backupQueue = backupQueue(ignite(1));

        assertEquals(0, backupQueue.size());

        List<Integer> keys = primaryKeys(cache, BACKUP_ACK_THRESHOLD);

        CountDownLatch latch = new CountDownLatch(keys.size());

        lsnr.latch = latch;

        for (Integer key : keys) {
            log.info("Put: " + key);

            cache.put(key, key);
        }

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return backupQueue.isEmpty();
            }
        }, 3000);

        assertTrue("Backup queue is not cleared: " + backupQueue, backupQueue.size() < BACKUP_ACK_THRESHOLD);

        if (!latch.await(5, SECONDS))
            fail("Failed to wait for notifications [exp=" + keys.size() + ", left=" + lsnr.latch.getCount() + ']');

        cur.close();
    }

    /**
     * @param ignite Ignite.
     * @return Backup queue for test query.
     */
    private Collection<Object> backupQueue(Ignite ignite) {
        GridContinuousProcessor proc = ((IgniteKernal)ignite).context().continuous();

        ConcurrentMap<Object, Object> infos = GridTestUtils.getFieldValue(proc, "rmtInfos");

        Collection<Object> backupQueue = null;

        for (Object info : infos.values()) {
            GridContinuousHandler hnd = GridTestUtils.getFieldValue(info, "hnd");

            if (hnd.isForQuery() && hnd.cacheName() == null) {
                backupQueue = GridTestUtils.getFieldValue(hnd, "backupQueue");

                break;
            }
        }

        assertNotNull(backupQueue);

        return backupQueue;
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailover() throws Exception {
        final int SRV_NODES = 4;

        startGridsMultiThreaded(SRV_NODES);

        client = true;

        Ignite qryClient = startGrid(SRV_NODES);

        client = false;

        IgniteCache<Object, Object> qryClientCache = qryClient.cache(null);

        final CacheEventListener2 lsnr = new CacheEventListener2();

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setLocalListener(lsnr);

        QueryCursor<?> cur = qryClientCache.query(qry);

        final AtomicBoolean stop = new AtomicBoolean();

        final AtomicReference<CountDownLatch> checkLatch = new AtomicReference<>();

        IgniteInternalFuture<?> restartFut = GridTestUtils.runAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                final int idx = SRV_NODES + 1;

                while (!stop.get() && !err) {
                    log.info("Start node: " + idx);

                    startGrid(idx);

                    Thread.sleep(3000);

                    log.info("Stop node: " + idx);

                    stopGrid(idx);

                    CountDownLatch latch = new CountDownLatch(1);

                    assertTrue(checkLatch.compareAndSet(null, latch));

                    if (!stop.get()) {
                        log.info("Wait for event check.");

                        assertTrue(latch.await(1, MINUTES));
                    }
                }

                return null;
            }
        });

        final Map<Integer, Integer> vals = new HashMap<>();

        final Map<Integer, List<T2<Integer, Integer>>> expEvts = new HashMap<>();

        try {
            long stopTime = System.currentTimeMillis() + 3 * 60_000;

            final int PARTS = qryClient.affinity(null).partitions();

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (System.currentTimeMillis() < stopTime) {
                Integer key = rnd.nextInt(PARTS);

                Integer prevVal = vals.get(key);
                Integer val = vals.get(key);

                if (val == null)
                    val = 0;
                else
                    val = val + 1;

                qryClientCache.put(key, val);

                vals.put(key, val);

                List<T2<Integer, Integer>> keyEvts = expEvts.get(key);

                if (keyEvts == null) {
                    keyEvts = new ArrayList<>();

                    expEvts.put(key, keyEvts);
                }

                keyEvts.add(new T2<>(val, prevVal));

                CountDownLatch latch = checkLatch.get();

                if (latch != null) {
                    log.info("Check events.");

                    checkLatch.set(null);

                    boolean success = false;

                    try {
                        if (err)
                            break;

                        boolean check = GridTestUtils.waitForCondition(new GridAbsPredicate() {
                            @Override public boolean apply() {
                                return checkEvents(false, expEvts, lsnr);
                            }
                        }, 10_000);

                        if (!check)
                            assertTrue(checkEvents(true, expEvts, lsnr));

                        success = true;

                        log.info("Events checked.");
                    }
                    finally {
                        if (!success)
                            err = true;

                        latch.countDown();
                    }
                }
            }
        }
        finally {
            stop.set(true);
        }

        CountDownLatch latch = checkLatch.get();

        if (latch != null)
            latch.countDown();

        restartFut.get();

        boolean check = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                return checkEvents(false, expEvts, lsnr);
            }
        }, 10_000);

        if (!check)
            assertTrue(checkEvents(true, expEvts, lsnr));

        cur.close();

        assertFalse("Unexpected error during test, see log for details.", err);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultiThreaded() throws Exception {
        final int SRV_NODES = 3;

        startGridsMultiThreaded(SRV_NODES);

        client = true;

        Ignite qryClient = startGrid(SRV_NODES);

        final IgniteCache<Object, Object> cache = qryClient.cache(null);

        CacheEventListener1 lsnr = new CacheEventListener1(true);

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setLocalListener(lsnr);

        QueryCursor<?> cur = cache.query(qry);

        client = false;

        final int SRV_IDX = SRV_NODES - 1;

        List<Integer> keys = primaryKeys(ignite(SRV_IDX).cache(null), 10);

        final int THREADS = 10;

        for (int i = 0; i < keys.size(); i++) {
            log.info("Iteration: " + i);

            Ignite srv = ignite(SRV_IDX);

            TestCommunicationSpi spi = (TestCommunicationSpi)srv.configuration().getCommunicationSpi();

            spi.sndFirstOnly = new AtomicBoolean(false);

            final Integer key = keys.get(i);

            final AtomicInteger val = new AtomicInteger();

            CountDownLatch latch = new CountDownLatch(THREADS);

            lsnr.latch = latch;

            IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Integer val0 = val.getAndIncrement();

                    cache.put(key, val0);

                    return null;
                }
            }, THREADS, "update-thread");

            fut.get();

            stopGrid(SRV_IDX);

            if (!latch.await(5, SECONDS))
                fail("Failed to wait for notifications [exp=" + THREADS + ", left=" + lsnr.latch.getCount() + ']');

            assertEquals(THREADS, lsnr.allEvts.size());

            Set<Integer> vals = new HashSet<>();

            boolean err = false;

            for (CacheEntryEvent<?, ?> evt : lsnr.allEvts) {
                assertEquals(key, evt.getKey());
                assertNotNull(evt.getValue());

                if (!vals.add((Integer)evt.getValue())) {
                    err = true;

                    log.info("Extra event: " + evt);
                }
            }

            for (int v = 0; v < THREADS; v++) {
                if (!vals.contains(v)) {
                    err = true;

                    log.info("Event for value not received: " + v);
                }
            }

            assertFalse("Invalid events, see log for details.", err);

            lsnr.allEvts.clear();

            startGrid(SRV_IDX);
        }

        cur.close();
    }

    /**
     * @param logAll If {@code true} logs all unexpected values.
     * @param expEvts Expected values.
     * @param lsnr Listener.
     * @return Check status.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private boolean checkEvents(boolean logAll,
        Map<Integer, List<T2<Integer, Integer>>> expEvts,
        CacheEventListener2 lsnr) {
        assertTrue(!expEvts.isEmpty());

        boolean pass = true;

        for (Map.Entry<Integer, List<T2<Integer, Integer>>> e : expEvts.entrySet()) {
            Integer key = e.getKey();
            List<T2<Integer, Integer>> exp = e.getValue();

            List<CacheEntryEvent<?, ?>> rcvdEvts = lsnr.evts.get(key);

            if (rcvdEvts == null) {
                pass = false;

                log.info("No events for key [key=" + key + ", exp=" + e.getValue() + ']');

                if (!logAll)
                    return false;
            }
            else {
                synchronized (rcvdEvts) {
                    if (rcvdEvts.size() != exp.size()) {
                        pass = false;

                        log.info("Missed or extra events for key [key=" + key +
                            ", exp=" + e.getValue() +
                            ", rcvd=" + rcvdEvts + ']');

                        if (!logAll)
                            return false;
                    }

                    int cnt = Math.min(rcvdEvts.size(), exp.size());

                    for (int i = 0; i < cnt; i++) {
                        T2<Integer, Integer> expEvt = exp.get(i);
                        CacheEntryEvent<?, ?> rcvdEvt = rcvdEvts.get(i);

                        assertEquals(key, rcvdEvt.getKey());
                        assertEquals(expEvt.get1(), rcvdEvt.getValue());
                        assertEquals(expEvt.get2(), rcvdEvt.getOldValue());
                    }
                }
            }
        }

        if (pass) {
            expEvts.clear();
            lsnr.evts.clear();
        }

        return pass;
    }

    /**
     *
     */
    private static class CacheEventListener1 implements CacheEntryUpdatedListener<Object, Object> {
        /** */
        private volatile CountDownLatch latch;

        /** */
        private GridConcurrentHashSet<Integer> keys = new GridConcurrentHashSet<>();

        /** */
        private ConcurrentHashMap<Object, CacheEntryEvent<?, ?>> evts = new ConcurrentHashMap<>();

        /** */
        private List<CacheEntryEvent<?, ?>> allEvts;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /**
         * @param saveAll Save all events flag.
         */
        CacheEventListener1(boolean saveAll) {
            if (saveAll)
                allEvts = new ArrayList<>();
        }

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
            try {
                for (CacheEntryEvent<?, ?> evt : evts) {
                    CountDownLatch latch = this.latch;

                    log.info("Received cache event [evt=" + evt +
                        ", left=" + (latch != null ? latch.getCount() : null) + ']');

                    this.evts.put(evt.getKey(), evt);

                    keys.add((Integer) evt.getKey());

                    if (allEvts != null)
                        allEvts.add(evt);

                    assertTrue(latch != null);
                    assertTrue(latch.getCount() > 0);

                    latch.countDown();

                    if (latch.getCount() == 0) {
                        this.latch = null;

                        keys.clear();
                    }
                }
            }
            catch (Throwable e) {
                err = true;

                log.error("Unexpected error", e);
            }
        }
    }

    /**
     *
     */
    private static class CacheEventListener2 implements CacheEntryUpdatedListener<Object, Object> {
        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private final ConcurrentHashMap<Integer, Integer> vals = new ConcurrentHashMap<>();

        /** */
        private final ConcurrentHashMap<Integer, List<CacheEntryEvent<?, ?>>> evts = new ConcurrentHashMap<>();

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts)
            throws CacheEntryListenerException  {
            try {
                for (CacheEntryEvent<?, ?> evt : evts) {
                    Integer key = (Integer)evt.getKey();
                    Integer val = (Integer)evt.getValue();

                    assertNotNull(key);
                    assertNotNull(val);

                    Integer prevVal = vals.get(key);

                    boolean dup = false;

                    if (prevVal != null) {
                        if (prevVal.equals(val)) // Can get this event with automatic put retry.
                            dup = true;
                        else {
                            assertEquals("Unexpected event: " + evt, (Integer)(prevVal + 1), val);
                            assertEquals("Unexpected event: " + evt, prevVal, evt.getOldValue());
                        }
                    }
                    else {
                        assertEquals("Unexpected event: " + evt, (Object)0, val);
                        assertNull("Unexpected event: " + evt, evt.getOldValue());
                    }

                    if (!dup) {
                        vals.put(key, val);

                        List<CacheEntryEvent<?, ?>> keyEvts = this.evts.get(key);

                        if (keyEvts == null) {
                            keyEvts = Collections.synchronizedList(new ArrayList<CacheEntryEvent<?, ?>>());

                            this.evts.put(key, keyEvts);
                        }

                        keyEvts.add(evt);
                    }
                }
            }
            catch (Throwable e) {
                err = true;

                log.error("Unexpected error", e);
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
        private volatile boolean skipMsg;

        /** */
        private volatile AtomicBoolean sndFirstOnly;

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            Object msg0 = ((GridIoMessage)msg).message();

            if (msg0 instanceof GridContinuousMessage) {
                if (skipMsg) {
                    log.info("Skip continuous message: " + msg0);

                    return;
                }
                else {
                    AtomicBoolean sndFirstOnly = this.sndFirstOnly;

                    if (sndFirstOnly != null && !sndFirstOnly.compareAndSet(false, true)) {
                        log.info("Skip continuous message: " + msg0);

                        return;
                    }
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
