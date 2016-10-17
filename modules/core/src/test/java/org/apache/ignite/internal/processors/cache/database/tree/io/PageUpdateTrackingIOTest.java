package org.apache.ignite.internal.processors.cache.database.tree.io;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import junit.framework.TestCase;

import static org.apache.ignite.internal.processors.cache.database.tree.io.PageUpdateTrackingIO.*;

/**
 *
 */
public class PageUpdateTrackingIOTest extends TestCase {
    /** Page size. */
    public static final int PAGE_SIZE = 2048;

    /**
     *
     */
    public void testBasics() {
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);

        markChanged(buf, 2, 0, PAGE_SIZE);

        assertTrue(wasChanged(buf, 2, 0, PAGE_SIZE));

        assertFalse(wasChanged(buf, 1, 0, PAGE_SIZE));
        assertFalse(wasChanged(buf, 3, 0, PAGE_SIZE));
        assertFalse(wasChanged(buf, 2, 1, PAGE_SIZE));
    }

    /**
     *
     */
    public void testRandom() {
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);

        int cntOfPageToTrack = countOfPageToTrack(PAGE_SIZE);

        for (int i = 0; i < 1001; i++)
            checkRandomly(buf, cntOfPageToTrack, i);
    }

    /**
     * @param buf Buffer.
     * @param track Track.
     * @param backupId Backup id.
     */
    private void checkRandomly(ByteBuffer buf, int track, int backupId) {
        ThreadLocalRandom rand = ThreadLocalRandom.current();

        long basePageId = (Math.max(rand.nextLong(Integer.MAX_VALUE - track), 0) / track) * track;

        assert basePageId >= 0;

        PageIO.setPageId(buf, basePageId);

        Map<Long, Boolean> map = new HashMap<>();

        int cntOfChanged = 0;

        try {
            for (long i = basePageId; i < basePageId + track; i++) {
                boolean changed =  i == basePageId || rand.nextDouble() < 0.5;

                map.put(i, changed);

                if (changed) {
                    markChanged(buf, i, backupId, PAGE_SIZE);

                    cntOfChanged++;
                }

                assertEquals(basePageId, PageIO.getPageId(buf));
                assertEquals(cntOfChanged, countOfChangedPage(buf, backupId, PAGE_SIZE));
            }

            assertEquals(cntOfChanged, countOfChangedPage(buf, backupId, PAGE_SIZE));

            for (Map.Entry<Long, Boolean> e : map.entrySet())
                assertEquals(
                    e.getValue().booleanValue(),
                    wasChanged(buf, e.getKey(), backupId, PAGE_SIZE));
        }
        catch (Throwable e) {
            System.out.println("backupId = " + backupId + ", basePageId = " + basePageId);
            throw e;
        }
    }
}