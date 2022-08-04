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
package org.apache.ignite.internal.processors.cache.persistence.db.wal;

import java.util.BitSet;
import java.util.Collections;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.WalSegmentCompactedEvent;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.pagemem.wal.IgniteWriteAheadLogManager;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_COMPACTED;

/**
 * WAL compaction notification test.
 */
public class WalCompactionNotificationsTest extends GridCommonAbstractTest {
    /** Wal segment size. */
    private static final int SEGMENT_SIZE = 512 * 1024;

    /** */
    private final ListeningTestLogger logger = new ListeningTestLogger(log);

    /** */
    private final EventListener evtLsnr = new EventListener();

    /** Wal archive size. */
    private long archiveSize = DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration()
                .setPersistenceEnabled(true)
                .setMaxSize(200L * 1024 * 1024))
            .setWalSegmentSize(SEGMENT_SIZE)
            .setWalCompactionEnabled(true)
            .setMaxWalArchiveSize(archiveSize)
        );

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
            .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
            .setAffinity(new RendezvousAffinityFunction(false, 16))
        );

        cfg.setGridLogger(logger);
        cfg.setConsistentId(name);
        cfg.setIncludeEventTypes(EVT_WAL_SEGMENT_COMPACTED);
        cfg.setLocalEventListeners(Collections.singletonMap(evtLsnr, new int[] {EVT_WAL_SEGMENT_COMPACTED}));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Throwable If failed.
     */
    @Test
    public void testOrder() throws Throwable {
        NotificationIndexListener notifyStartLsnr = new NotificationIndexListener(evtLsnr.errRef, evtLsnr.enqueueHist, true);
        NotificationIndexListener notifyEndLsnr = new NotificationIndexListener(evtLsnr.errRef, evtLsnr.compressedHist, false);

        logger.registerListener(notifyStartLsnr);
        logger.registerListener(notifyEndLsnr);

        IgniteEx ig = startGrid(0);
        ig.cluster().state(ClusterState.ACTIVE);

        doCachePuts(ig, 100);
        evtLsnr.checkErrors();

        ig.cluster().state(ClusterState.INACTIVE);
        stopGrid(0);

        IgniteEx ig0 = startGrid(0);
        ig0.cluster().state(ClusterState.ACTIVE);

        doCachePuts(ig0, 100);
        evtLsnr.checkErrors();
    }

    /** */
    private void doCachePuts(IgniteEx ig, int segmentsCnt) {
        IgniteCache<Integer, String> cache = ig.cache(DEFAULT_CACHE_NAME);
        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();
        long totalIdx = walMgr.currentSegment() + segmentsCnt;

        while (evtLsnr.errRef.get() == null && !Thread.currentThread().isInterrupted() && walMgr.currentSegment() < totalIdx)
            cache.put(ThreadLocalRandom.current().nextInt(), "Ignite");
    }

    /** */
    private static class NotificationIndexListener extends LogListener {
        /** Message pattern. */
        private final Pattern pattern;

        /** Index history. */
        private final BitSet idxHistory;

        /** Error reference. */
        private final AtomicReference<Throwable> errRef;

        /**
         * @param errRef Error reference.
         * @param idxHistory Index history.
         * @param startNotification Flag indicating that we are catching compression start messages.
         */
        private NotificationIndexListener(AtomicReference<Throwable> errRef, BitSet idxHistory, boolean startNotification) {
            this.errRef = errRef;
            this.idxHistory = idxHistory;

            pattern = startNotification ?
                Pattern.compile("Segment compressed notification \\[idx=(-?\\d{1,10})\\]") :
                Pattern.compile("Enqueuing segment for compression \\[idx=(-?\\d{1,10})\\]");
        }

        /** {@inheritDoc} */
        @Override public boolean check() {
            throw new UnsupportedOperationException();
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            idxHistory.clear();
        }

        /** {@inheritDoc} */
        @Override public void accept(String s) {
            try {
                Matcher matcher = pattern.matcher(s);

                if (!matcher.find())
                    return;

                int idx = Integer.parseInt(matcher.group(1));

                assertTrue("Negative index value in log [idx=" + idx + ", msg=" + s + ']', idx >= 0);
                assertFalse("Duplicate index in log [idx=" + idx + ", msg=" + s + ']', idxHistory.get(idx));

                idxHistory.set(idx);
            }
            catch (Error | RuntimeException e) {
                errRef.compareAndSet(null, e);

                throw e;
            }
        }
    }

    /** */
    private class EventListener implements IgnitePredicate<Event> {
        /** Error. */
        private final AtomicReference<Throwable> errRef = new AtomicReference<>();

        /** Event history. */
        private final BitSet evtHistory = new BitSet();

        /** Log compress start notification history. */
        private final BitSet enqueueHist = new BitSet();

        /** Log compression end notification history. */
        private final BitSet compressedHist = new BitSet();

        /** Last compacted segment index. */
        private long lastCompactedSegment;

        /** Last event index. */
        private long lastEvtIdx;

        /** {@inheritDoc} */
        @Override public boolean apply(Event evt) {
            try {
                if (!(evt instanceof WalSegmentCompactedEvent))
                    fail("Unexpected event type: " + evt.getClass().getName());

                WalSegmentCompactedEvent compactEvt = (WalSegmentCompactedEvent)evt;
                IgniteWriteAheadLogManager walMgr = grid(0).context().cache().context().wal();

                long lastCompactedSegment = walMgr.lastCompactedSegment();
                assertTrue("Negative index: " + lastCompactedSegment, lastCompactedSegment >= 0);

                if (this.lastCompactedSegment > lastCompactedSegment) {
                    fail("Unordered last compact segment value " +
                        "[prev=" + this.lastCompactedSegment + ", curr=" + walMgr.lastCompactedSegment() + ']');
                }

                this.lastCompactedSegment = lastCompactedSegment;

                int walIdx = (int)compactEvt.getAbsWalSegmentIdx();

                if (evtHistory.get(walIdx))
                    fail("Duplicate event [idx=" + walIdx + ']');

                evtHistory.set(walIdx);

                lastEvtIdx = compactEvt.getAbsWalSegmentIdx();

                return true;
            }
            catch (Throwable t) {
                errRef.set(t);

                return false;
            }
        }

        /**
         * @throws Throwable If failed.
         */
        public void checkErrors() throws Throwable {
            Throwable err = errRef.get();

            if (err != null)
                throw err;

            IgniteWriteAheadLogManager walMgr = grid(0).context().cache().context().wal();
            GridTestUtils.waitForCondition(() -> lastEvtIdx == walMgr.lastCompactedSegment(), getTestTimeout());
            assertEquals(lastEvtIdx, walMgr.lastCompactedSegment());

            int lastCompactedIdx = (int)walMgr.lastCompactedSegment();

            for (int i = 0; i < lastCompactedIdx; i++) {
                assertTrue("Missing event [idx=" + i + ']', evtHistory.get(i));
                assertTrue("Log compression start missing [idx=" + i + ']', enqueueHist.get(i));
                assertTrue("Log compression end missing [idx=" + i + ']', compressedHist.get(i));
            }
        }
    }
}
