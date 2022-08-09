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
import java.util.EnumMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_WAL_SEGMENT_COMPACTED;

/**
 * WAL compaction notification test.
 */
public class WalCompactionNotificationsTest extends GridCommonAbstractTest {
    /** Wal segment size. */
    private static final int SEGMENT_SIZE = 512 * 1024;

    /** Listening logger. */
    private final ListeningTestLogger logger = new ListeningTestLogger(log);

    /** WAL compaction event listener. */
    private final EventListener evtLsnr = new EventListener();

    /** WAL archive size. */
    private long archiveSize;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        return super.getConfiguration(name)
            .setDataStorageConfiguration(new DataStorageConfiguration()
                .setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration()
                        .setPersistenceEnabled(true)
                        .setMaxSize(200 * U.MB))
                .setWalSegmentSize(SEGMENT_SIZE)
                .setMaxWalArchiveSize(archiveSize)
                .setWalCompactionEnabled(true)
            ).setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setAffinity(new RendezvousAffinityFunction(false, 16))
            ).setGridLogger(logger)
            .setConsistentId(name)
            .setIncludeEventTypes(EVT_WAL_SEGMENT_COMPACTED)
            .setLocalEventListeners(Collections.singletonMap(evtLsnr, new int[] {EVT_WAL_SEGMENT_COMPACTED}));
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
    public void testNotificationsUnlimitedWal() throws Throwable {
        archiveSize = DataStorageConfiguration.UNLIMITED_WAL_ARCHIVE;

        logger.registerListener(evtLsnr);

        checkRestart(50);
        checkRestart(50);
    }

    /**
     * @throws Throwable If failed.
     */
    @Test
    public void testNotificationsEmptyArchive() throws Throwable {
        archiveSize = SEGMENT_SIZE;

        logger.registerListener(evtLsnr);

        checkRestart(50);
        checkRestart(0);
    }

    /**
     * @param segmentsCnt The number of WAL segments to generate.
     * @throws Exception If failed.
     */
    private void checkRestart(int segmentsCnt) throws Exception {
        IgniteEx ig = startGrid(0);
        ig.cluster().state(ClusterState.ACTIVE);

        generateWal(ig, segmentsCnt);
        ig.cluster().state(ClusterState.INACTIVE);

        evtLsnr.validateEvents();

        stopGrid(0);
    }

    /**
     * @param ig Ignite instance.
     * @param segmentsCnt The number of WAL segments to generate.
     */
    private void generateWal(IgniteEx ig, int segmentsCnt) {
        if (segmentsCnt <= 0)
            return;

        IgniteCache<Integer, String> cache = ig.cache(DEFAULT_CACHE_NAME);
        IgniteWriteAheadLogManager walMgr = ig.context().cache().context().wal();
        long totalIdx = walMgr.currentSegment() + segmentsCnt;

        while (evtLsnr.errRef.get() == null && !Thread.currentThread().isInterrupted() && walMgr.currentSegment() < totalIdx)
            cache.put(ThreadLocalRandom.current().nextInt(), "Ignite");
    }

    /** Log event type. */
    enum LogEventType {
        /** */
        ENQUEUE("Enqueuing segment for compression"),

        /** */
        SKIP("Skipping segment compression"),

        /** */
        COMPRESS("Segment compressed notification");

        /** */
        private final Pattern ptrn;

        /** */
        LogEventType(String msg) {
            ptrn = Pattern.compile(msg + " \\[idx=(?<idx>-?\\d{1,10})]");
        }
    }

    /** WAL compaction event listener. */
    private class EventListener implements IgnitePredicate<Event>, Consumer<String> {
        /** Error. */
        private final AtomicReference<Throwable> errRef = new AtomicReference<>();

        /** Events history. */
        private final BitSet evtHistory = new BitSet();

        /** Log notifications history. */
        private final EnumMap<LogEventType, BitSet> logEvtHist = new EnumMap<>(LogEventType.class);

        /** Last compacted segment index. */
        private long lastCompactedSegment;

        /** Constructor. */
        public EventListener() {
            for (LogEventType type : LogEventType.values())
                logEvtHist.put(type, new BitSet());
        }

        /** {@inheritDoc} */
        @SuppressWarnings("ErrorNotRethrown")
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

                return true;
            }
            catch (AssertionError | RuntimeException e) {
                errRef.compareAndSet(null, e);

                return false;
            }
        }

        /** {@inheritDoc} */
        @SuppressWarnings("ErrorNotRethrown")
        @Override public void accept(String str) {
            try {
                for (LogEventType type : LogEventType.values()) {
                    Matcher matcher = type.ptrn.matcher(str);

                    if (!matcher.find())
                        continue;

                    int idx = Integer.parseInt(matcher.group("idx"));

                    assertTrue("Negative index value in log [idx=" + idx + ", msg=" + str + ']', idx >= 0);

                    BitSet hist = logEvtHist.get(type);

                    if (type == LogEventType.ENQUEUE && logEvtHist.get(LogEventType.SKIP).get(idx))
                        break;

                    assertFalse("Duplicate index in log [idx=" + idx + ", msg=" + str + ']', hist.get(idx));

                    hist.set(idx);

                    break;
                }
            }
            catch (AssertionError | RuntimeException e) {
                errRef.compareAndSet(null, e);
            }
        }

        /**
         * @throws AssertionError If failed.
         */
        public void validateEvents() {
            Throwable err = errRef.get();

            if (err instanceof AssertionError)
                throw (AssertionError)err;

            if (err instanceof RuntimeException)
                throw (RuntimeException)err;

            if (err != null)
                fail("Unexpected exception [class=" + err.getClass().getName() + ", msg=" + err.getMessage() + "].");

            int lastCompactedIdx = (int)grid(0).context().cache().context().wal().lastCompactedSegment();
            BitSet enqHist = logEvtHist.get(LogEventType.ENQUEUE);
            BitSet cmprsHist = logEvtHist.get(LogEventType.COMPRESS);
            BitSet skipHist = logEvtHist.get(LogEventType.SKIP);

            for (int i = 0; i < lastCompactedIdx; i++) {
                assertTrue("Missing event [idx=" + i + ']', evtHistory.get(i) == cmprsHist.get(i));
                assertTrue("Log compression start missing [idx=" + i + ']', enqHist.get(i));
                assertTrue("Log compression end missing [idx=" + i + ']', cmprsHist.get(i) || skipHist.get(i));
            }
        }
    }
}
