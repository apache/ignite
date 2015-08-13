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

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cache.affinity.*;
import org.apache.ignite.cache.query.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.processors.continuous.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import javax.cache.event.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

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
    public void testOneBackup() throws Exception {
        checkBackupQueue(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testThreeBackups() throws Exception {
        if (cacheMode() == REPLICATED)
            return;

        checkBackupQueue(3);
    }

    /**
     * @param backups Number of backups.
     * @throws Exception If failed.
     */
    private void checkBackupQueue(int backups) throws Exception {
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

        CacheEventListener1 lsnr = new CacheEventListener1();

        ContinuousQuery<Object, Object> qry = new ContinuousQuery<>();

        qry.setLocalListener(lsnr);

        QueryCursor<?> cur = qryClientCache.query(qry);

        int PARTS = 10;

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

                cache.put(key, key);
            }

            if (!latch.await(5, SECONDS)) {
                Set<Integer> keys0 = new HashSet<>(keys);

                keys0.removeAll(lsnr.keys);

                log.info("Missed events for keys: " + keys0);

                fail("Failed to wait for notifications [exp=" + keys.size() + ", left=" + lsnr.latch.getCount() + ']');
            }
        }

        cur.close();

        assertFalse("Unexpected error during test, see log for details.", err);
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

        CacheEventListener1 lsnr = new CacheEventListener1();

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

        assertTrue("Backup queue is not cleared: " + backupQueue, backupQueue.isEmpty());

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

        CacheEventListener1 lsnr = new CacheEventListener1();

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

        assertTrue("Backup queue is not cleared: " + backupQueue, backupQueue.isEmpty());

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
    public void _testFailover() throws Exception {
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

                    Thread.sleep(2000);

                    log.info("Stop node: " + idx);

                    stopGrid(idx);

                    Thread.sleep(1000);

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

        try {
            long stopTime = System.currentTimeMillis() + 3 * 60_000;

            final int PARTS = qryClient.affinity(null).partitions();

            ThreadLocalRandom rnd = ThreadLocalRandom.current();

            while (System.currentTimeMillis() < stopTime) {
                Integer key = rnd.nextInt(PARTS);

                Integer val = vals.get(key);

                if (val == null)
                    val = 0;
                else
                    val = val + 1;

                qryClientCache.put(key, val);

                vals.put(key, val);

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
                                return checkEvents(false, vals, lsnr);
                            }
                        }, 10_000);

                        if (!check)
                            assertTrue(checkEvents(true, vals, lsnr));

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
                return checkEvents(false, vals, lsnr);
            }
        }, 10_000);

        if (!check)
            assertTrue(checkEvents(true, vals, lsnr));

        cur.close();

        assertFalse("Unexpected error during test, see log for details.", err);
    }

    /**
     * @param logAll If {@code true} logs all unexpected values.
     * @param vals Expected values.
     * @param lsnr Listener.
     * @return Check status.
     */
    private boolean checkEvents(boolean logAll, Map<Integer, Integer> vals, CacheEventListener2 lsnr) {
        assertTrue(!vals.isEmpty());

        ConcurrentHashMap<Integer, Integer> lsnrVals = lsnr.vals;

        ConcurrentHashMap<Integer, Integer> lsnrCntrs = lsnr.cntrs;

        boolean pass = true;

        for (Map.Entry<Integer, Integer> e : vals.entrySet()) {
            Integer key = e.getKey();

            Integer lsnrVal = lsnrVals.get(key);
            Integer expVal = e.getValue();

            if (!expVal.equals(lsnrVal)) {
                pass = false;

                log.info("Unexpected value [key=" + key + ", val=" + lsnrVal + ", expVal=" + expVal + ']');

                if (!logAll)
                    return false;
            }

            Integer lsnrCntr = lsnrCntrs.get(key);
            Integer expCntr = expVal + 1;

            if (!expCntr.equals(lsnrCntr)) {
                pass = false;

                log.info("Unexpected events count [key=" + key + ", val=" + lsnrCntr + ", expVal=" + expCntr + ']');

                if (!logAll)
                    return false;
            }
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
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public void onUpdated(Iterable<CacheEntryEvent<?, ?>> evts) {
            try {
                for (CacheEntryEvent<?, ?> evt : evts) {
                    CountDownLatch latch = this.latch;

                    log.info("Received cache event: " + evt + " " + (latch != null ? latch.getCount() : null));

                    keys.add((Integer) evt.getKey());

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
        private final ConcurrentHashMap<Integer, Integer> cntrs = new ConcurrentHashMap<>();

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

                    if (prevVal != null) {
                        assertEquals("Unexpected event: " + evt, (Integer)(prevVal + 1), val);
                        assertEquals("Unexpected event: " + evt, prevVal, evt.getOldValue());
                    }
                    else {
                        assertEquals("Unexpected event: " + evt, (Object)0, val);
                        assertNull("Unexpected event: " + evt, evt.getOldValue());
                    }

                    vals.put(key, val);

                    Integer cntr = cntrs.get(key);

                    if (cntr == null)
                        cntr = 1;
                    else
                        cntr = cntr + 1;

                    cntrs.put(key, cntr);
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

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
            throws IgniteSpiException {
            if (skipMsg && msg instanceof GridIoMessage) {
                Object msg0 = ((GridIoMessage)msg).message();

                if (msg0 instanceof GridContinuousMessage) {
                    log.info("Skip continuous message: " + msg0);

                    return;
                }
            }

            super.sendMessage(node, msg, ackC);
        }
    }
}
