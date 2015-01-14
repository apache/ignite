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

package org.apache.ignite.streamer.index;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.streamer.index.hash.*;
import org.apache.ignite.streamer.index.tree.*;
import org.apache.ignite.streamer.window.*;
import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.streamer.index.StreamerIndexPolicy.*;
import static org.gridgain.testframework.GridTestUtils.*;

/**
 * Tests for Streamer window index.
 */
public class GridStreamerIndexSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testTreeIndex() throws Exception {
        for (StreamerIndexPolicy plc : StreamerIndexPolicy.values()) {
            checkUniqueIndex(indexProvider(true, "idx", new UniqueStringIndexUpdater(), plc, true));

            checkNonUniqueIndex(indexProvider(true, "idx", new IndexUpdater(), plc, false));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testHashIndex() throws Exception {
        for (StreamerIndexPolicy plc : StreamerIndexPolicy.values()) {
            checkUniqueIndex(indexProvider(false, "idx", new UniqueStringIndexUpdater(), plc, true));

            checkNonUniqueIndex(indexProvider(false, "idx", new IndexUpdater(), plc, false));
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMultipleIndexUpdate() throws Exception {
        StreamerIndexProvider<String, String, Integer> idxProvider =
            indexProvider(true, "idx", new IndexUpdater(), EVENT_TRACKING_ON, false);

        StreamerIndexProvider<String, String, String> idxProvider1 =
            indexProvider(true, "idx1", new UniqueStringIndexUpdater(), EVENT_TRACKING_ON, true);

        StreamerBoundedSizeWindow<String> win = new StreamerBoundedSizeWindow<>();

        win.setMaximumSize(5);
        win.setIndexes(idxProvider, idxProvider1);

        win.start();

        win.enqueue("A");
        win.enqueue("B");
        win.enqueue("C");
        win.enqueue("D");

        // Snapshot both indexes.
        StreamerIndex<String, String, Integer> idx = win.index("idx");
        StreamerIndex<String, String, String> idx1 = win.index("idx1");

        info("Idx: " + idx.entries(0));
        info("Idx1: " + idx1.entries(0));

        try {
            win.enqueue("A");

            fail("Exception should have been thrown.");
        }
        catch (IgniteCheckedException e) {
            info("Caught expected exception: " + e);
        }

        StreamerIndex<String, String, Integer> idxAfter = win.index("idx");
        StreamerIndex<String, String, String> idx1After = win.index("idx1");

        info("Idx (after): " + idxAfter.entries(0));
        info("Idx1 (after): " + idx1After.entries(0));

        assertEquals(4, idx.entries(0).size());
        assertEquals(4, idx1.entries(0).size());

        assertTrue(F.eqOrdered(idx.entries(0), idxAfter.entries(0)));
        assertTrue(F.eqOrdered(idx1.entries(0), idx1After.entries(0)));

        idxProvider.reset();

        assertEquals(4, idx.entries(0).size());
    }

    /**
     * @throws Exception If failed.
     */
    public void testSortedIndexMultithreaded() throws Exception {
        checkSortedIndexMultithreaded(32, 500, false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSortedIndexMultithreadedWithConcurrentPollEvicted() throws Exception {
        checkSortedIndexMultithreaded(32, 500, true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUniqueHashIndexMultithreaded() throws Exception {
        checkUniqueHashIndexMultithreaded(32, 500);
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdaterIndexKeyNull() throws Exception {
        checkIndexUpdater(new IndexUpdater() {
            @Nullable @Override public String indexKey(String evt) {
                return "A".equals(evt) ? null : evt;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdaterInitialValueNull() throws Exception {
        checkIndexUpdater(new IndexUpdater() {
            @Nullable @Override public Integer initialValue(String evt, String key) {
                return "A".equals(evt) ? null : 1;
            }
        });
    }

    /**
     * @throws Exception If failed.
     */
    public void testUpdaterOnAddedNull() throws Exception {
        checkIndexUpdater(new IndexUpdater() {
            @Nullable @Override
            public Integer onAdded(StreamerIndexEntry<String, String, Integer> entry, String evt) {
                return "A".equals(evt) ? null : entry.value() + 1;
            }
        });
    }

    /**
     * Checks the correct behaviour of {@link StreamerIndexUpdater}, given that
     * it discards event "A" and accepts event "B".
     *
     * @param updater Index updater.
     * @throws IgniteCheckedException If failed.
     */
    private void checkIndexUpdater(StreamerIndexUpdater<String, String, Integer> updater) throws IgniteCheckedException {
        List<StreamerIndexProvider<String, String, Integer>> idxps = Arrays.asList(
            indexProvider(true, "tree", updater, StreamerIndexPolicy.EVENT_TRACKING_ON, false),
            indexProvider(false, "hash", updater, StreamerIndexPolicy.EVENT_TRACKING_ON, false));

        for (StreamerIndexProvider<String, String, Integer> idxp : idxps) {
            StreamerUnboundedWindow<String> win = new StreamerUnboundedWindow<>();

            win.setIndexes(idxp);

            win.start();

            win.enqueue("A");
            win.enqueue("A");
            win.enqueue("B");

            StreamerIndex<String, Object, Object> idx = win.index(idxp.getName());

            assertNotNull(idx);

            assertNull(idx.entry("A"));

            assertNotNull(idx.entry("B"));
        }
    }

    /**
     * @param treeIdx {@code True} to create tree index.
     * @param name Name.
     * @param updater Updater.
     * @param plc Policy.
     * @param unique Unique.
     * @return Index provider.
     */
    private <E, K, V> StreamerIndexProvider<E, K, V> indexProvider(boolean treeIdx, String name,
        StreamerIndexUpdater<E, K, V> updater, StreamerIndexPolicy plc, boolean unique) {
        if (treeIdx) {
            StreamerTreeIndexProvider<E, K, V> idx = new StreamerTreeIndexProvider<>();

            idx.setName(name);
            idx.setUpdater(updater);
            idx.setUnique(unique);
            idx.setPolicy(plc);

            return idx;
        }
        else {
            StreamerHashIndexProvider<E, K, V> idx = new StreamerHashIndexProvider<>();

            idx.setName(name);
            idx.setUpdater(updater);
            idx.setUnique(unique);
            idx.setPolicy(plc);

            return idx;
        }
    }

    /**
     * @param threadCnt Thread count.
     * @param iters Number of iterations for each worker thread.
     * @throws Exception If failed.
     */
    private void checkUniqueHashIndexMultithreaded(int threadCnt, final int iters)
        throws Exception {
        StreamerIndexProvider<String, String, Integer> idxProvider =
            indexProvider(false, "idx", new IndexUpdater(), EVENT_TRACKING_ON_DEDUP, true);

        for (int i = 0; i < iters && !Thread.currentThread().isInterrupted(); i++) {
            final StreamerBoundedSizeWindow<String> win = new StreamerBoundedSizeWindow<>();

            win.setMaximumSize(threadCnt * 2);
            win.setIndexes(idxProvider);

            win.start();

            final String evt = "evt" + i;
            final AtomicInteger nIdxErrors = new AtomicInteger();

            // Submit the same event in multiple threads.
            runMultiThreaded(new CAX() {
                @Override public void applyx() throws IgniteCheckedException {
                    try {
                        win.enqueue(evt);
                    }
                    catch (IgniteCheckedException e) {
                        if (e.getMessage().contains("Index unique key violation"))
                            nIdxErrors.incrementAndGet();
                        else
                            throw e;
                    }
                }
            }, threadCnt, "put");

            // Only one thread should succeed, because the index is unique.
            assertEquals(threadCnt - 1, nIdxErrors.get());

            StreamerIndex<String, String, Integer> idx = win.index("idx");

            // Only one event should be present and have value 1.
            assertEquals(1, idx.entries(0).size());
            assertEquals((Integer)1, idx.entry(evt).value());
        }
    }

    /**
     * @param threadCnt Thread count.
     * @param iters Number of iterations for each worker thread.
     * @param pollEvicted Poll evicted events concurrently, if true.
     * @throws Exception If failed.
     */
    public void checkSortedIndexMultithreaded(final int threadCnt, final int iters, final boolean pollEvicted)
        throws Exception {
        final StreamerBoundedSizeWindow<String> win = new StreamerBoundedSizeWindow<>();

        win.setMaximumSize(threadCnt * 2);
        win.setIndexes(indexProvider(true, "idx", new IndexUpdater(), EVENT_TRACKING_ON_DEDUP, false));

        win.start();

        IgniteFuture<Long> pollFut = null;

        if (pollEvicted) {
            // These threads poll evicted events from the window if it doesn't break
            // the test invariant.
            pollFut = runMultiThreadedAsync(new CAX() {
                @Override public void applyx() throws IgniteCheckedException {
                    try {
                        while (!Thread.currentThread().isInterrupted()) {
                            StreamerIndex<String, String, Integer> idx = win.index("idx");

                            boolean canPoll = F.forAll(
                                idx.entries(-1 * threadCnt),
                                new P1<StreamerIndexEntry<String, String, Integer>>() {
                                    @Override public boolean apply(StreamerIndexEntry<String, String, Integer> e) {
                                        return e.value() > 2;
                                    }
                                });

                            if (!canPoll || win.pollEvicted() == null)
                                U.sleep(50);
                        }
                    }
                    catch (GridInterruptedException ignored) {
                        // No-op.
                    }
                }
            }, threadCnt / 4, "test-poll");
        }

        try {
            // Each of these threads generates a single event repeatedly and checks
            // if it is still present in the window. In the tested index events are
            // sorted by value and the value is a number of repeated events, so, this
            // should be invariant.
            IgniteFuture<Long> fut1 = runMultiThreadedAsync(new CAX() {
                @Override public void applyx() throws IgniteCheckedException {
                    final String evt = Thread.currentThread().getName();
                    int cntr = 1;

                    for (int i = 0; i < iters && !Thread.currentThread().isInterrupted(); i++) {
                        win.enqueue(evt);

                        StreamerIndex<String, String, Integer> idx = win.index("idx");
                        StreamerIndexEntry<String, String, Integer> entry = idx.entry(evt);

                        assertNotNull(entry);

                        // If concurrent eviction is disabled, check if the
                        // value grows each time we enqueue a new event.
                        if (!pollEvicted)
                            assertEquals((Integer)cntr++, entry.value());

                        // If queued event more than once, the first threadCnt entries
                        // in descending order should contain an entry with this thread's event.
                        if (i > 0)
                            assert idx.entries(-1 * threadCnt).contains(entry);
                    }
                }
            }, threadCnt / 2, "test-multi");

            // This thread generates a set of single non-repeating events from 0 to iters.
            IgniteFuture<Long> fut2 = runMultiThreadedAsync(new CAX() {
                @Override public void applyx() throws IgniteCheckedException {
                    for (int i = 0; i < iters && !Thread.currentThread().isInterrupted(); i++)
                        win.enqueue(String.valueOf(i));
                }
            }, 1, "test-single");

            fut2.get(getTestTimeout());
            fut1.get(getTestTimeout());
        }
        finally {
            if (pollFut != null)
                pollFut.cancel();
        }
    }

    /**
     * @param idx Index.
     * @throws IgniteCheckedException If failed.
     */
    private void checkNonUniqueIndex(StreamerIndexProvider<String, String, Integer> idx) throws IgniteCheckedException {
        assert !idx.isUnique();

        StreamerBoundedSizeWindow<String> win = new StreamerBoundedSizeWindow<>();

        win.setMaximumSize(5);
        win.setIndexes(idx);

        win.start();

        for (int i = 0; i < 20; ) {
            win.enqueue("A" + i); i++;
            win.enqueue("B"); i++;
            win.enqueue("C"); i++;
            win.enqueue("D"); i++;
        }

        StreamerIndex<String, String, Integer> idx0 = win.index("idx");

        String s;

        while ((s = win.pollEvicted()) != null)
            info("Evicted String: " + s);

        StreamerIndex<String, String, Integer> idx1 = win.index("idx");

        if (idx instanceof StreamerTreeIndexProvider) { // Tree index.
            assert idx0.sorted();

            // Users with unique names.
            for (StreamerIndexEntry<String, String, Integer> e : idx0.entrySet(1)) {
                info("Entry [e=" + e + ", evts=" + e.events() + ']');

                if (idx.getPolicy() == EVENT_TRACKING_ON || idx.getPolicy() == EVENT_TRACKING_ON_DEDUP) {
                    assertEquals(1, e.events().size());
                    assertEquals('A', F.first(e.events()).charAt(0));
                }
            }

            assertTrue(idx0.entrySet(2).isEmpty());

            for (StreamerIndexEntry<String, String, Integer> e : idx0.entrySet(5)) {
                info("Entry [e=" + e + ", evts=" + e.events() + ']');

                if (idx.getPolicy() == EVENT_TRACKING_ON)
                    assertEquals(5, e.events().size());

                else if (idx.getPolicy() == EVENT_TRACKING_ON_DEDUP)
                    assertEquals(1, e.events().size());

                else
                    assertNull(e.events());
            }

            assertEquals(5, idx0.entrySet(1).size());

            List<StreamerIndexEntry<String, String, Integer>> asc =
                new ArrayList<>(idx0.entrySet(true, null, true, null, true));
            List<StreamerIndexEntry<String, String, Integer>> desc =
                new ArrayList<>(idx0.entrySet(false, null, true, null, true));

            assertEquals(8, asc.size());
            assertEquals(8, desc.size());

            for (int i = 0; i < asc.size(); i++)
                assertEquals(asc.get(i), desc.get(desc.size() - i - 1));

            try {
                idx0.entrySet(true, 10, true, -10, true);

                assert false;
            }
            catch (IllegalArgumentException e) {
                info("Caught expected exception: " + e);
            }

            try {
                idx0.entrySet(false, -10, true, 10, true);

                assert false;
            }
            catch (IllegalArgumentException e) {
                info("Caught expected exception: " + e);
            }
        }
        else
            assert !idx0.sorted();

        assertEquals(4, idx1.size());

        for (StreamerIndexEntry<String, String, Integer> e : idx1.entries(0)) {
            Collection<String> evts = e.events();

            info("Entry [e=" + e + ", evts=" + evts + ']');

            if (idx.getPolicy() == EVENT_TRACKING_ON) {
                assert evts != null;

                switch (evts.size()) {
                    case 1:
                        assert F.containsAny(evts, "A16", "B", "C") : "Wrong tracked event: " + F.first(evts);

                        break;

                    case 2:
                        Collection<String> dedup = F.dedup(evts);

                        assert dedup.size() == 1 && "D".equals(F.first(dedup)) : "Wrong tracked events: " + evts;

                        break;

                    default:
                        fail("Wrong tracked events: " + evts);
                }
            }
            else if (idx.getPolicy() == EVENT_TRACKING_ON_DEDUP)
                assert evts != null && evts.size() == 1 && F.containsAny(evts, "A16", "B", "C", "D") :
                    "Wrong tracked events: " + evts;
            else if (idx.getPolicy() == EVENT_TRACKING_OFF)
                assert evts == null;
        }

        // Check that idx0 is unaffected.
        assertEquals(8, idx0.size());

        idx.reset();

        assertEquals(0, idx.index().size());
        assertEquals(8, idx0.size());
    }

    /**
     * @param idx Index.
     * @throws IgniteCheckedException If failed.
     */
    private void checkUniqueIndex(StreamerIndexProvider<String, String, String> idx) throws IgniteCheckedException {
        assert idx.isUnique();

        StreamerBoundedSizeWindow<String> win = new StreamerBoundedSizeWindow<>();

        win.setMaximumSize(5);
        win.setIndexes(idx);

        win.start();

        for (int i = 0; i < 20; i++)
            win.enqueue("A" + i);

        for (int i = 0; i < 20; i++) {
            try {
                win.enqueue("A" + i);

                fail("Exception should have been thrown.");
            }
            catch (IgniteCheckedException e) {
                info("Caught expected exception: " + e);
            }
        }

        StreamerIndex<String, String, String> idx0 = win.index("idx");

        String s;

        while ((s = win.pollEvicted()) != null)
            info("Evicted string: " + s);

        StreamerIndex<String, String, String> idx1 = win.index("idx");

        if (idx instanceof StreamerTreeIndexProvider) { // Tree index.
            assert idx0.sorted();

            // Users with unique names.
            for (StreamerIndexEntry<String, String, String> e : idx0.entrySet("A0")) {
                info("Entry [e=" + e + ", evts=" + e.events() + ']');

                if (idx.getPolicy() == EVENT_TRACKING_ON || idx.getPolicy() == EVENT_TRACKING_ON_DEDUP) {
                    assertEquals(1, e.events().size());
                    assertEquals('A', F.first(e.events()).charAt(0));
                }
            }

            assertTrue(idx0.entrySet("B").isEmpty());

            assertEquals(1, idx0.entrySet("A0").size());

            List<StreamerIndexEntry<String, String, String>> asc =
                new ArrayList<>(idx0.entrySet(true, null, true, null, true));
            List<StreamerIndexEntry<String, String, String>> desc =
                new ArrayList<>(idx0.entrySet(false, null, true, null, true));

            assertEquals(20, asc.size());
            assertEquals(20, desc.size());

            for (int i = 0; i < asc.size(); i++)
                assertEquals(asc.get(i), desc.get(desc.size() - i - 1));
        }
        else
            assert !idx0.sorted();

        assertEquals(5, idx1.size());

        for (StreamerIndexEntry<String, String, String> e : idx1.entries(0)) {
            Collection<String> evts = e.events();

            info("Entry [e=" + e + ", evts=" + evts + ']');

            if (idx.getPolicy() == EVENT_TRACKING_ON || idx.getPolicy() == EVENT_TRACKING_ON_DEDUP) {
                assert evts != null && evts.size() == 1 : "Wrong tracked events: " + evts;

                int i = Integer.parseInt(F.first(evts).substring(1));

                assert i >= 15 && i < 20 : "Wrong event: " + F.first(evts);
            }
            else if (idx.getPolicy() == EVENT_TRACKING_OFF)
                assert evts == null;
        }

        // Check that idx0 is unaffected.
        assertEquals(20, idx0.size());

        idx.reset();

        assertEquals(0, idx.index().size());
        assertEquals(20, idx0.size());
    }

    /**
     * Name index updater.
     */
    private static class IndexUpdater implements StreamerIndexUpdater<String, String, Integer> {
        /** {@inheritDoc} */
        @Nullable @Override public String indexKey(String evt) {
            return evt;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Integer initialValue(String evt, String key) {
            return 1;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Integer onAdded(StreamerIndexEntry<String, String, Integer> entry, String evt) {
            return entry.value() + 1;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Integer onRemoved(StreamerIndexEntry<String, String, Integer> entry,
            String evt) {
            int res = entry.value() - 1;

            return res == 0 ? null : res;
        }
    }

    /**
     * Name index updater.
     */
    private static class HashIndexUpdater implements StreamerIndexUpdater<String, String, Integer> {
        /** {@inheritDoc} */
        @Nullable @Override public String indexKey(String evt) {
            return evt;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Integer initialValue(String evt, String key) {
            return 1;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Integer onAdded(StreamerIndexEntry<String, String, Integer> entry, String evt) {
            return entry.value() + 1;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Integer onRemoved(StreamerIndexEntry<String, String, Integer> entry,
            String evt) {
            int res = entry.value() - 1;

            return res == 0 ? null : res;
        }
    }

    /**
     * Name index updater.
     */
    private static class UniqueStringIndexUpdater implements StreamerIndexUpdater<String, String, String> {
        /** {@inheritDoc} */
        @Nullable @Override public String indexKey(String evt) {
            return evt;
        }

        /** {@inheritDoc} */
        @Nullable @Override public String initialValue(String evt, String key) {
            return evt;
        }

        /** {@inheritDoc} */
        @Nullable @Override public String onAdded(StreamerIndexEntry<String, String, String> entry, String evt)
            throws IgniteCheckedException {
            throw new IgniteCheckedException("Unique key violation: " + evt);
        }

        /** {@inheritDoc} */
        @Nullable @Override public String onRemoved(StreamerIndexEntry<String, String, String> entry,
            String evt) {
            // On remove we return null as index is unique.
            return null;
        }
    }
}
