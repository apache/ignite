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

package org.apache.ignite.loadtests.continuous;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.CacheEntryUpdatedListener;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicWriteOrderMode;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMemoryMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.logger.NullLogger;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Yet another failover test for continuous queries.
 */
public abstract class CacheContinuousQueryAbstractLoadTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost("127.0.0.1");

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(cacheMode());
        ccfg.setAtomicityMode(atomicityMode());
        ccfg.setAtomicWriteOrderMode(writeOrderMode());
        ccfg.setBackups(backups());
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setMemoryMode(CacheMemoryMode.ONHEAP_TIERED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @return Number of backups.
     */
    protected abstract int backups();

    /**
     * @return Cache mode.
     */
    protected abstract CacheMode cacheMode();

    /**
     * @return Atomicity mode.
     */
    protected abstract CacheAtomicityMode atomicityMode();

    /**
     * @return Write order mode.
     */
    protected abstract CacheAtomicWriteOrderMode writeOrderMode();

    /**
     * This is failover test detecting CQ event loss while topology changing.
     *
     * @throws Exception If failed.
     */
    public void testLocalCQEventLossOnTopologyChanging() throws Exception {
        final int stableNodeCnt = 1;

        final int batchLoadSize = 2000;

        Ignite qryClient = startGridsMultiThreaded(stableNodeCnt);

        final CacheEventListener lsnr = new CacheEventListener(atomicityMode() == CacheAtomicityMode.ATOMIC);

        ContinuousQuery<Integer, Integer> qry = new ContinuousQuery<>();

        qry.setLocalListener(lsnr);

        IgniteCache<Integer, Integer> cache = qryClient.cache(null);

        QueryCursor<?> cur = cache.query(qry);

        int iteration = 0;

        int putCnt = 0;

        int ignoredDupEvents = 0;

        Thread nodeRestartThread = nodeRestartThread(stableNodeCnt, 5, 2_000, 2_000);

        try {
            nodeRestartThread.start();

            while (!Thread.interrupted() && nodeRestartThread.isAlive()) {
                iteration++;

                for (int i = 0; i < batchLoadSize; i++)
                    cache.put(i, iteration);

                putCnt += batchLoadSize;

                log.info("Batch loaded. Iteration: " + iteration);

                final long cnt = lsnr.count();

                final long expCnt = putCnt * stableNodeCnt + ignoredDupEvents;

                GridTestUtils.waitForCondition(new GridAbsPredicate() {
                    @Override public boolean apply() {
                        return cnt == expCnt;
                    }
                }, 6_000);

                if (cnt != expCnt) {
                    StringBuilder sb = new StringBuilder();

                    for (int i = 0; i < batchLoadSize; i++) {
                        Integer key = i;
                        Integer val = cache.get(key);

                        if (!F.eq(val, iteration))
                            sb.append("\n\t").append(">>> WRONG CACHE VALUE (lost data?) [key=").append(key)
                                .append(", val=").append(val).append(']');
                    }

                    for (Map.Entry<Integer, Integer> entry : lsnr.eventMap().entrySet()) {
                        Integer key = entry.getKey();
                        Integer val = entry.getValue();

                        if (!F.eq(val, iteration))
                            sb.append("\n\t").append(">>> WRONG LISTENER VALUE (lost event?) [key=").append(key)
                                .append(", val=").append(val).append(']');
                    }

                    String msg = sb.toString();

                    // In atomic mode CQ can receive duplicate update events if update retried after fails.
                    // E.g. topology change
                    if (atomicityMode() == CacheAtomicityMode.ATOMIC && msg.isEmpty() && cnt == expCnt + 1)
                        ignoredDupEvents++;
                    else
                        fail("Unexpected event updates count: EXPECTED=" + expCnt + ", ACTUAL=" + cnt + ", " +
                            "ITERATION=" + iteration + msg);
                }

                sleep(500);
            }
        }
        finally {
            nodeRestartThread.interrupt();

            cur.close();

            nodeRestartThread.join(3_000);
        }
    }

    /**
     * Start thread which restarts a node over and over again.
     */
    @SuppressWarnings("InfiniteLoopStatement")
    public Thread nodeRestartThread(final int no, final int restartCycles,
        final long initialDelay, final long restartDelay) {

        final IgniteLogger log = this.log;

        Thread t = new Thread(new Runnable() {
            public void run() {
                sleep(initialDelay);

                try {
                    for (int i = 1; i <= restartCycles && !Thread.interrupted(); i++) {

                        IgniteConfiguration cfg = getConfiguration(getTestGridName(no)).setGridLogger(new NullLogger());

                        log.debug("Node restart cycle started: " + i);

                        try (Ignite ignored = Ignition.start(cfg)) {
                            sleep(restartDelay);
                        }

                        log.info("Node restart cycle finished: " + i);

                        sleep(restartDelay);
                    }
                }
                catch (Exception e) {
                    log.error("Unexpected error.", e);
                }
            }
        });

        t.setName("flapping-node-thread");

        t.setDaemon(true);

        return t;
    }

    /**
     * Sleep quietly
     *
     * @param sleepTime Sleep time.
     */
    private void sleep(long sleepTime) {
        try {
            if (Thread.currentThread().isInterrupted())
                return;

            U.sleep(sleepTime);
        }
        catch (IgniteInterruptedCheckedException e) {
            Thread.interrupted();
        }
    }

    /**
     * Listener.
     */
    private static class CacheEventListener implements CacheEntryUpdatedListener<Integer, Integer> {
        /** Listener count. */
        private final AtomicLong counter = new AtomicLong();

        /** Listener map. */
        private final Map<Integer, Integer> eventMap = new ConcurrentHashMap<>();

        /** Atomicity mode flag. */
        private final boolean atomicModeFlag;

        /** Constructor */
        public CacheEventListener(boolean atomicModeFlag) {
            this.atomicModeFlag = atomicModeFlag;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("EqualsBetweenInconvertibleTypes")
        @Override public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Integer>> evts)
            throws CacheEntryListenerException {
            for (CacheEntryEvent<? extends Integer, ? extends Integer> evt : evts) {
                Integer prev = eventMap.put(evt.getKey(), evt.getValue());

                //Atomic cache allows duplicate events if cache update operation fails, e.g. due to topology change.
                if (!atomicModeFlag || prev == null || !prev.equals(evt))
                    counter.incrementAndGet();
            }
        }

        /**
         * @return Events count.
         */
        public long count() {
            return counter.get();
        }

        /**
         * @return Event map.
         */
        Map<Integer, Integer> eventMap() {
            return eventMap;
        }
    }

}
