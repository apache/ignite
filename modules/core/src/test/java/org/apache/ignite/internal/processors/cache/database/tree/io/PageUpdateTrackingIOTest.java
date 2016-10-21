package org.apache.ignite.internal.processors.cache.database.tree.io;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;
import junit.framework.TestCase;

/**
 *
 */
public class PageUpdateTrackingIOTest extends TestCase {
    /** Page size. */
    public static final int PAGE_SIZE = 2048;

    private final PageUpdateTrackingIO io = PageUpdateTrackingIO.VERSIONS.latest();

    /**
     *
     */
    public void testBasics() {
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);

        io.markChanged(buf, 2, 0, PAGE_SIZE);

        assertTrue(io.wasChanged(buf, 2, 0, PAGE_SIZE));

        assertFalse(io.wasChanged(buf, 1, 0, PAGE_SIZE));
        assertFalse(io.wasChanged(buf, 3, 0, PAGE_SIZE));
        assertFalse(io.wasChanged(buf, 2, 1, PAGE_SIZE));
    }

    /**
     *
     */
    public void testMarkingRandomly() {
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);

        int cntOfPageToTrack = io.countOfPageToTrack(PAGE_SIZE);

        for (int i = 0; i < 1001; i++)
            checkMarkingRandomly(buf, cntOfPageToTrack, i, false);
    }

    /**
     *
     */
    public void testZeroingRandomly() {
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);

        int cntOfPageToTrack = io.countOfPageToTrack(PAGE_SIZE);

        for (int i = 0; i < 1001; i++)
            checkMarkingRandomly(buf, cntOfPageToTrack, i, true);
    }

    /**
     * @param buf Buffer.
     * @param track Track.
     * @param backupId Backup id.
     */
    private void checkMarkingRandomly(ByteBuffer buf, int track, int backupId, boolean testZeroing) {
        ThreadLocalRandom rand = ThreadLocalRandom.current();

        long basePageId = io.trackingPageFor(Math.max(rand.nextLong(Integer.MAX_VALUE - track), 0), PAGE_SIZE);

        long maxId = testZeroing ? basePageId + rand.nextInt(1, track) : basePageId + track;

        assert basePageId >= 0;

        PageIO.setPageId(buf, basePageId);

        Map<Long, Boolean> map = new HashMap<>();

        int cntOfChanged = 0;

        try {
            for (long i = basePageId; i < basePageId + track; i++) {
                boolean changed =  (i == basePageId || rand.nextDouble() < 0.5) && i < maxId;

                map.put(i, changed);

                if (changed) {
                    io.markChanged(buf, i, backupId, PAGE_SIZE);

                    cntOfChanged++;
                }

                assertEquals(basePageId, PageIO.getPageId(buf));
                assertEquals(cntOfChanged, io.countOfChangedPage(buf, backupId, PAGE_SIZE));
            }

            assertEquals(cntOfChanged, io.countOfChangedPage(buf, backupId, PAGE_SIZE));

            for (Map.Entry<Long, Boolean> e : map.entrySet())
                assertEquals(
                    e.getValue().booleanValue(),
                    io.wasChanged(buf, e.getKey(), backupId, PAGE_SIZE));
        }
        catch (Throwable e) {
            System.out.println("backupId = " + backupId + ", basePageId = " + basePageId);
            throw e;
        }
    }

    public void testFindNextChangedPage() throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);

        int cntOfPageToTrack = io.countOfPageToTrack(PAGE_SIZE);

        for (int i = 0; i < 101; i++)
            checkFindingRandomly(buf, cntOfPageToTrack, i);
    }

    /**
     * @param buf Buffer.
     * @param track Track.
     * @param backupId Backup id.
     */
    private void checkFindingRandomly(ByteBuffer buf, int track, int backupId) {
        ThreadLocalRandom rand = ThreadLocalRandom.current();

        PageUpdateTrackingIO io = PageUpdateTrackingIO.VERSIONS.latest();

        long basePageId = io.trackingPageFor(Math.max(rand.nextLong(Integer.MAX_VALUE - track), 0), PAGE_SIZE);

        long maxId = basePageId + rand.nextInt(1, track);

        assert basePageId >= 0;

        PageIO.setPageId(buf, basePageId);

        SortedMap<Long, Boolean> map = new TreeMap<>();

        TreeSet<Long> setIdx = new TreeSet<>();

        try {
            for (long i = basePageId; i < basePageId + track; i++) {
                boolean changed =  (i == basePageId || rand.nextDouble() < 0.2) && i < maxId;

                map.put(i, changed);

                if (changed) {
                    io.markChanged(buf, i, backupId, PAGE_SIZE);

                    setIdx.add(i);
                }
            }

            for (Map.Entry<Long, Boolean> e : map.entrySet()) {
                Long pageId = e.getKey();

                Long foundNextChangedPage = io.findNextChangedPage(buf, pageId, backupId, PAGE_SIZE);

                if (io.trackingPageFor(pageId, PAGE_SIZE) == pageId)
                    assertEquals(pageId, foundNextChangedPage);

                else if (e.getValue())
                    assertEquals(pageId, foundNextChangedPage);

                else {
                    NavigableSet<Long> tailSet = setIdx.tailSet(pageId, false);
                    Long next = tailSet.isEmpty() ? null : tailSet.first();

                    assertEquals(next, foundNextChangedPage);
                }
            }

        }
        catch (Throwable e) {
            System.out.println("backupId = " + backupId + ", basePageId = " + basePageId);
            throw e;
        }
    }
}