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

package org.apache.ignite.internal.processors.cache.persistence.tree.io;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.snapshot.TrackingPageIsCorruptedException;
import org.apache.ignite.internal.util.GridUnsafe;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 *
 */
public class TrackingPageIOTest {
    /** Page size. */
    public static final int PAGE_SIZE = 4096;

    /** */
    private final TrackingPageIO io = TrackingPageIO.VERSIONS.latest();

    /**
     *
     */
    @Test
    public void testBasics() throws Exception {
        ByteBuffer buf = createBuffer();

        io.markChanged(buf, 2, 0, -1, PAGE_SIZE);

        assertTrue(io.wasChanged(buf, 2, 0, -1, PAGE_SIZE));

        assertFalse(io.wasChanged(buf, 1, 0, -1, PAGE_SIZE));
        assertFalse(io.wasChanged(buf, 3, 0, -1, PAGE_SIZE));
        assertFalse(io.wasChanged(buf, 2, 1, 0, PAGE_SIZE));
    }

    /**
     * @return byte buffer with right order.
     */
    @NotNull private ByteBuffer createBuffer() {
        ByteBuffer buf = ByteBuffer.allocateDirect(PAGE_SIZE);

        buf.order(ByteOrder.nativeOrder());

        return buf;
    }

    /**
     *
     */
    @Test
    public void testMarkingRandomly() throws Exception {
        ByteBuffer buf = createBuffer();

        for (int i = 0; i < 1001; i++)
            checkMarkingRandomly(buf, i, false);
    }

    /**
     *
     */
    @Test
    public void testZeroingRandomly() throws Exception {
        ByteBuffer buf = createBuffer();

        for (int i = 0; i < 1001; i++)
            checkMarkingRandomly(buf, i, true);
    }

    /**
     * @param buf Buffer.
     * @param backupId Backup id.
     */
    private void checkMarkingRandomly(ByteBuffer buf, int backupId, boolean testZeroing) throws Exception {
        ThreadLocalRandom rand = ThreadLocalRandom.current();

        int track = io.countOfPageToTrack(PAGE_SIZE);

        long basePageId = io.trackingPageFor(Math.max(rand.nextLong(Integer.MAX_VALUE - track), 0), PAGE_SIZE);

        long maxId = testZeroing ? basePageId + rand.nextInt(1, track) : basePageId + track;

        assert basePageId >= 0;

        PageIO.setPageId(GridUnsafe.bufferAddress(buf), basePageId);

        Map<Long, Boolean> map = new HashMap<>();

        int cntOfChanged = 0;

        try {
            for (long i = basePageId; i < basePageId + track; i++) {
                boolean changed = (i == basePageId || rand.nextDouble() < 0.5) && i < maxId;

                map.put(i, changed);

                if (changed) {
                    io.markChanged(buf, i, backupId, backupId - 1, PAGE_SIZE);

                    cntOfChanged++;
                }

                assertEquals(basePageId, PageIO.getPageId(buf));
                assertEquals(cntOfChanged, io.countOfChangedPage(buf, backupId, PAGE_SIZE));
            }

            assertEquals(cntOfChanged, io.countOfChangedPage(buf, backupId, PAGE_SIZE));

            for (Map.Entry<Long, Boolean> e : map.entrySet())
                assertEquals(
                    e.getValue().booleanValue(),
                    io.wasChanged(buf, e.getKey(), backupId, backupId - 1, PAGE_SIZE));
        }
        catch (Throwable e) {
            System.out.println("snapshotId = " + backupId + ", basePageId = " + basePageId);
            throw e;
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFindNextChangedPage() throws Exception {
        ByteBuffer buf = createBuffer();

        for (int i = 0; i < 101; i++)
            checkFindingRandomly(buf, i);
    }

    /**
     * @param buf Buffer.
     * @param backupId Backup id.
     */
    private void checkFindingRandomly(ByteBuffer buf, int backupId) throws Exception {
        ThreadLocalRandom rand = ThreadLocalRandom.current();

        int track = io.countOfPageToTrack(PAGE_SIZE);

        long basePageId = io.trackingPageFor(Math.max(rand.nextLong(Integer.MAX_VALUE - track), 0), PAGE_SIZE);

        long maxId = basePageId + rand.nextInt(1, track);

        assert basePageId >= 0;

        PageIO.setPageId(GridUnsafe.bufferAddress(buf), basePageId);

        try {
            TreeSet<Long> setIdx = new TreeSet<>();

            generateMarking(buf, track, basePageId, maxId, setIdx, backupId, backupId - 1);

            for (long pageId = basePageId; pageId < basePageId + track; pageId++) {
                Long foundNextChangedPage = io.findNextChangedPage(buf, pageId, backupId, backupId - 1, PAGE_SIZE);

                if (io.trackingPageFor(pageId, PAGE_SIZE) == pageId)
                    assertEquals((Long) pageId, foundNextChangedPage);

                else if (setIdx.contains(pageId))
                    assertEquals((Long) pageId, foundNextChangedPage);

                else {
                    NavigableSet<Long> tailSet = setIdx.tailSet(pageId, false);
                    Long next = tailSet.isEmpty() ? null : tailSet.first();

                    assertEquals(next, foundNextChangedPage);
                }
            }
        }
        catch (Throwable e) {
            System.out.println("snapshotId = " + backupId + ", basePageId = " + basePageId);
            throw e;
        }
    }

    /**
     *
     */
    @Test
    public void testMerging() throws Exception {
        ByteBuffer buf = createBuffer();

        ThreadLocalRandom rand = ThreadLocalRandom.current();

        int track = io.countOfPageToTrack(PAGE_SIZE);

        long basePageId = io.trackingPageFor(Math.max(rand.nextLong(Integer.MAX_VALUE - track), 0), PAGE_SIZE);

        assert basePageId >= 0;

        PageIO.setPageId(GridUnsafe.bufferAddress(buf), basePageId);

        TreeSet<Long> setIdx = new TreeSet<>();

        for (int i = 0; i < 4; i++)
            generateMarking(buf, track, basePageId, basePageId + rand.nextInt(1, track), setIdx, i, -1);

        TreeSet<Long> setIdx2 = new TreeSet<>();

        generateMarking(buf, track, basePageId, basePageId + rand.nextInt(1, track), setIdx2, 4, -1);

        assertEquals(setIdx2.size(), io.countOfChangedPage(buf, 4, PAGE_SIZE));
        assertEquals(setIdx.size(), io.countOfChangedPage(buf, 3, PAGE_SIZE));

        for (long i = basePageId; i < basePageId + track; i++)
            assertEquals("pageId = " + i, setIdx.contains(i), io.wasChanged(buf, i, 3, -1, PAGE_SIZE));

        for (long i = basePageId; i < basePageId + track; i++)
            assertEquals("pageId = " + i, setIdx2.contains(i), io.wasChanged(buf, i, 4, 3, PAGE_SIZE));

        for (long i = basePageId; i < basePageId + track; i++)
            assertFalse(io.wasChanged(buf, i, 5, 4, PAGE_SIZE));
    }

    /**
     *
     */
    @Test
    public void testMerging_MarksShouldBeDropForSuccessfulBackup() throws Exception {
        ByteBuffer buf = createBuffer();

        ThreadLocalRandom rand = ThreadLocalRandom.current();

        int track = io.countOfPageToTrack(PAGE_SIZE);

        long basePageId = io.trackingPageFor(Math.max(rand.nextLong(Integer.MAX_VALUE - track), 0), PAGE_SIZE);

        assert basePageId >= 0;

        PageIO.setPageId(GridUnsafe.bufferAddress(buf), basePageId);

        TreeSet<Long> setIdx = new TreeSet<>();

        for (int i = 0; i < 4; i++)
            generateMarking(buf, track, basePageId, basePageId + rand.nextInt(1, track), setIdx, i, -1);

        setIdx.clear();

        generateMarking(buf, track, basePageId, basePageId + rand.nextInt(1, track), setIdx, 4, -1);

        TreeSet<Long> setIdx2 = new TreeSet<>();

        generateMarking(buf, track, basePageId, basePageId + rand.nextInt(1, track), setIdx2, 5, 3);

        assertEquals(setIdx.size(), io.countOfChangedPage(buf, 4, PAGE_SIZE));
        assertEquals(setIdx2.size(), io.countOfChangedPage(buf, 5, PAGE_SIZE));

        for (long i = basePageId; i < basePageId + track; i++)
            assertEquals("pageId = " + i, setIdx2.contains(i), io.wasChanged(buf, i, 5, 4, PAGE_SIZE));
    }

    /**
     * @param buf Buffer.
     * @param track Track.
     * @param basePageId Base page id.
     * @param maxPageId Max page id.
     * @param setIdx Set index.
     * @param backupId Backup id.
     * @param successfulBackupId Successful backup id.
     */
    private void generateMarking(
        ByteBuffer buf,
        int track,
        long basePageId,
        long maxPageId,
        Set<Long> setIdx,
        int backupId, int successfulBackupId
    ) {
        ThreadLocalRandom rand = ThreadLocalRandom.current();

        for (long i = basePageId; i < basePageId + track; i++) {
            boolean changed = (i == basePageId || rand.nextDouble() < 0.1) && i < maxPageId;

            if (changed) {
                io.markChanged(buf, i, backupId, successfulBackupId, PAGE_SIZE);

                setIdx.add(i);
            }
        }
    }

    /**
     * We should handle case when we lost snapshot tag and now it's lower than saved.
     *
     * @throws Exception if failed.
     */
    @Test
    public void testThatWeDontFailIfSnapshotTagWasLost() throws Exception {
        ByteBuffer buf = createBuffer();

        long basePageId = PageIdUtils.pageId(0, PageIdAllocator.FLAG_IDX, 1);

        assert basePageId >= 0;

        PageIO.setPageId(GridUnsafe.bufferAddress(buf), basePageId);

        int oldTag = 10;

        io.markChanged(buf, basePageId + 1, oldTag, oldTag - 1, PAGE_SIZE);

        for (int i = 1; i < 100; i++)
            io.markChanged(buf, basePageId + i, oldTag - 1, oldTag - 2, PAGE_SIZE);

        assertTrue(io.isCorrupted(buf));

        for (int i = 1; i < 100; i++) {
            try {
                long id = basePageId + i;

                io.wasChanged(buf, id, oldTag - 1, oldTag - 2, PAGE_SIZE);

                fail();
            }
            catch (TrackingPageIsCorruptedException e) {
                //ignore
            }
        }

        for (int i = 1; i < 100; i++) {
            long id = basePageId + i + 1000;

            io.markChanged(buf, id, oldTag, oldTag - 2, PAGE_SIZE);
        }

        io.resetCorruptFlag(buf);

        assertFalse(io.isCorrupted(buf));

        for (int i = 1; i < 100; i++) {
            long id = basePageId + i + 1000;

            assertTrue(io.wasChanged(buf, id, oldTag, oldTag - 1, PAGE_SIZE));
        }

        for (int i = 1; i < 100; i++) {
            long id = basePageId + i;

            assertFalse(io.wasChanged(buf, id, oldTag, oldTag - 1, PAGE_SIZE));
        }
    }
}
