/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.streamer.window;

import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.streamer.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Streamer window self test.
 */
public class GridStreamerWindowSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testBoundedSizeWindowValidation() throws Exception {
        final StreamerBoundedSizeWindow win = new StreamerBoundedSizeWindow();

        win.start();

        win.setMaximumSize(-1);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                win.start();

                return null;
            }
        }, GridException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoundedTimeWindowValidation() throws Exception {
        final StreamerBoundedTimeWindow win = new StreamerBoundedTimeWindow();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                win.start();

                return null;
            }
        }, GridException.class, null);

        win.setTimeInterval(1);

        win.start();

        win.setMaximumSize(-1);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                win.start();

                return null;
            }
        }, GridException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoundedSizeBatchWindowValidation() throws Exception {
        final StreamerBoundedSizeBatchWindow win = new StreamerBoundedSizeBatchWindow();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                win.start();

                return null;
            }
        }, GridException.class, null);

        win.setBatchSize(1);

        win.start();

        win.setMaximumBatches(-1);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                win.start();

                return null;
            }
        }, GridException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoundedTimeBatchWindowValidation() throws Exception {
        final StreamerBoundedTimeBatchWindow win = new StreamerBoundedTimeBatchWindow();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                win.start();

                return null;
            }
        }, GridException.class, null);

        win.setBatchSize(1);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                win.start();

                return null;
            }
        }, GridException.class, null);

        win.setBatchTimeInterval(1);
        win.setBatchSize(-1);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                win.start();

                return null;
            }
        }, GridException.class, null);

        win.setBatchSize(1);

        win.start();

        win.setMaximumBatches(-1);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                win.start();

                return null;
            }
        }, GridException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoundedWindow() throws Exception {
        final StreamerBoundedSizeWindow<Integer> win = new StreamerBoundedSizeWindow<>();

        win.setMaximumSize(50);

        win.start();

        for (int i = 0; i < 50; i++)
            win.enqueue(i);

        assertNull(win.pollEvicted());

        for(int i = 50; i < 60; i++)
            win.enqueue(i);

        for (int i = 0; i < 10; i++)
            assert i == win.pollEvicted();

        assertNull(win.pollEvicted());

        checkIterator(win);

        win.setMaximumSize(2);

        win.start();

        win.enqueue(3, 2, 1);

        checkSnapshot(win.snapshot(true), 3, 2, 1);
        checkSnapshot(win.snapshot(false), 2, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoundedWindowUnique() throws Exception {
        final StreamerBoundedSizeWindow<Integer> win = new StreamerBoundedSizeWindow<>();

        win.setMaximumSize(50);
        win.setUnique(true);

        win.start();

        for (int i = 0; i < 50; i++)
            win.enqueue(i);

        for (int i = 0; i < 50; i++)
            win.enqueue(i);

        assertNull(win.pollEvicted());

        int idx = 0;

        for (Object evt : win) {
            Integer next = (Integer)evt;

            assertEquals((Integer)idx++, next);
        }

        checkIterator(win);

        win.setMaximumSize(2);

        win.start();

        win.enqueue(3, 2, 1, 3);

        checkSnapshot(win.snapshot(true), 3, 2, 1);
        checkSnapshot(win.snapshot(false), 2, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoundedSortedWindow() throws Exception {
        final StreamerBoundedSizeSortedWindow<Integer> win = new StreamerBoundedSizeSortedWindow<>();

        win.setMaximumSize(60);

        win.start();

        for (int i = 59; i >= 0; i--)
            win.enqueue(i);

        assertNull(win.pollEvicted());

        for (int i = 59; i >= 0; i--)
            win.enqueue(i);

        for (int i = 59; i >= 30; i--) {
            assert i == win.pollEvicted();
            assert i == win.pollEvicted();
        }

        assertNull(win.pollEvicted());

        checkIterator(win);

        win.setMaximumSize(2);

        win.start();

        win.enqueue(3, 2, 1, 4);

        checkSnapshot(win.snapshot(true), 1, 2, 3, 4);
        checkSnapshot(win.snapshot(false), 3, 4);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoundedSortedWindowUnique() throws Exception {
        final StreamerBoundedSizeSortedWindow<Integer> win = new StreamerBoundedSizeSortedWindow<>();

        win.setMaximumSize(-1);

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                win.start();

                return null;
            }
        }, GridException.class, null);

        win.setMaximumSize(60);
        win.setUnique(true);

        win.start();

        for (int i = 59; i >= 0; i--)
            win.enqueue(i);

        assertNull(win.pollEvicted());

        for (int i = 59; i >= 0; i--)
            win.enqueue(i);

        assertNull(win.pollEvicted());

        for (int i = 99; i >= 60; i--)
            win.enqueue(i);

        for (int i = 99; i >= 60; i--)
            assert i == win.pollEvicted();

        assertNull(win.pollEvicted());

        checkIterator(win);

        win.setMaximumSize(2);

        win.start();

        win.enqueue(3, 2, 1, 3, 4);

        checkSnapshot(win.snapshot(true), 1, 2, 3, 4);
        checkSnapshot(win.snapshot(false), 3, 4);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoundedSizeBatchDequeueWindow() throws Exception {
        final StreamerBoundedSizeBatchWindow<Integer> win = new StreamerBoundedSizeBatchWindow<>();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                win.start();

                return null;
            }
        }, GridException.class, null);

        win.setBatchSize(10);
        win.setMaximumBatches(2);

        win.start();

        for (int i = 0; i < 20; i++)
            win.enqueue(i);

        assertNull(win.pollEvicted());
        assertEquals(0, win.pollEvictedBatch().size());

        win.enqueue(20);

        Collection<Integer> evicted = win.pollEvictedBatch();

        assertEquals(10, evicted.size());

        Iterator<Integer> it = evicted.iterator();

        for (int i = 0; i < 10; i++)
            assert i == it.next();

        assertNull(win.pollEvicted());
        assertEquals(0, win.pollEvictedBatch().size());

        for (int i = 21; i < 30; i++)
            win.enqueue(i);

        assertNull(win.pollEvicted());
        assertEquals(0, win.pollEvictedBatch().size());

        win.enqueue(30);

        assert 10 == win.pollEvicted();

        evicted = win.pollEvictedBatch();

        assertEquals(9, evicted.size());

        it = evicted.iterator();

        for (int i = 11; i < 20; i++)
            assert i == it.next();

        assertNull(win.pollEvicted());
        assertEquals(0, win.pollEvictedBatch().size());

        checkIterator(win);

        win.setMaximumBatches(2);
        win.setBatchSize(2);

        win.start();

        win.enqueue(1, 2, 3, 4, 5, 6, 7);

        // We expect that the first two batches will be evicted.
        checkSnapshot(win.snapshot(true), 1, 2, 3, 4, 5, 6, 7);
        checkSnapshot(win.snapshot(false), 5, 6, 7);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoundedTimeDequeueWindow() throws Exception {
        final StreamerBoundedTimeWindow<Integer> win = new StreamerBoundedTimeWindow<>();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                win.start();

                return null;
            }
        }, GridException.class, null);

        win.setMaximumSize(60);
        win.setTimeInterval(40);

        win.start();

        for (int i = 59; i >= 0; i--)
            win.enqueue(i);

        assertNull(win.pollEvicted());

        for (int i = 59; i >= 0; i--)
            win.enqueue(i);

        for (int i = 59; i >= 0; i--)
            assert i == win.pollEvicted();

        assertNull(win.pollEvicted());

        checkIterator(win);

        win.setMaximumSize(2);
        win.setTimeInterval(200);

        win.start();

        win.enqueue(1, 2, 3);

        checkSnapshot(win.snapshot(true), 1, 2, 3);
        checkSnapshot(win.snapshot(false), 2, 3);

        U.sleep(400);

        win.enqueue(4);

        checkSnapshot(win.snapshot(true), 1, 2, 3, 4);
        checkSnapshot(win.snapshot(false), 4);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoundedTimeBatchDequeueWindow() throws Exception {
        final StreamerBoundedTimeBatchWindow<Integer> win = new StreamerBoundedTimeBatchWindow<>();

        GridTestUtils.assertThrows(log, new Callable<Object>() {
            @Override public Object call() throws Exception {
                win.start();

                return null;
            }
        }, GridException.class, null);

        win.setBatchSize(50);
        win.setBatchTimeInterval(500);
        win.setMaximumBatches(2);

        win.start();

        for (int i = 0; i < 25; i++)
            win.enqueue(i);

        U.sleep(1000);

        Collection<Integer> evicted = win.pollEvictedBatch();

        assertNotNull(evicted);
        assertEquals(25, evicted.size());

        for (int i = 0; i < 101; i++)
            win.enqueue(i);

        evicted = win.pollEvictedBatch();

        assertNotNull(evicted);
        assertEquals(50, evicted.size());

        U.sleep(1000);

        evicted = win.pollEvictedBatch();

        assertNotNull(evicted);
        assertEquals(50, evicted.size());

        evicted = win.pollEvictedBatch();

        assertNotNull(evicted);
        assertEquals(1, evicted.size());

        checkIterator(win);

        win.setMaximumBatches(2);
        win.setBatchSize(2);
        win.setBatchTimeInterval(200);

        win.start();

        win.enqueue(1, 2, 3, 4, 5, 6, 7);

        // We expect that the first two batches will be evicted.
        checkSnapshot(win.snapshot(true), 1, 2, 3, 4, 5, 6, 7);
        checkSnapshot(win.snapshot(false), 5, 6, 7);

        U.sleep(400);

        checkSnapshot(win.snapshot(true), 1, 2, 3, 4, 5, 6, 7);
        checkSnapshot(win.snapshot(false));
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnboundedDequeueWindow() throws Exception {
        final StreamerUnboundedWindow<Integer> win = new StreamerUnboundedWindow<>();

        win.start();

        for (int i = 0; i < 50; i++)
            win.enqueue(i);

        assertNull(win.pollEvicted());

        assert win.size() == 50;

        checkIterator(win);

        win.reset();

        win.enqueue(3, 1, 2);

        checkSnapshot(win.snapshot(true), 3, 1, 2);
        checkSnapshot(win.snapshot(false), 3, 1, 2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoundedSizeDequeueWindowMultithreaded() throws Exception {
        StreamerBoundedSizeWindow<Integer> win = new StreamerBoundedSizeWindow<>();

        win.setMaximumSize(500);
        win.setUnique(false);

        win.start();

        checkWindowMultithreaded(win, 100000, 10, 1000);

        win.consistencyCheck();

        finalChecks(win, 500);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoundedSizeDequeueWindowUniqueMultithreaded() throws Exception {
        StreamerBoundedSizeWindow<Integer> win = new StreamerBoundedSizeWindow<>();

        win.setMaximumSize(500);
        win.setUnique(true);

        win.start();

        checkWindowMultithreaded(win, 100000, 10, 1000);

        win.consistencyCheck();

        finalChecks(win, 500);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoundedSizeBatchDequeueWindowMultithreaded() throws Exception {
        StreamerBoundedSizeBatchWindow<Integer> win = new StreamerBoundedSizeBatchWindow<>();

        win.setMaximumBatches(10);
        win.setBatchSize(50);

        win.start();

        checkWindowMultithreaded(win, 100000, 10, 1000);

        win.consistencyCheck();

        finalChecks(win, 500);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoundedSizeSortedDequeueWindowMultithreaded() throws Exception {
        StreamerBoundedSizeSortedWindow<Integer> win = new StreamerBoundedSizeSortedWindow<>();

        win.setMaximumSize(500);
        win.setUnique(false);

        win.start();

        checkWindowMultithreaded(win, 100000, 10, 1000);

        win.consistencyCheck();

        finalChecks(win, 500);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoundedSizeSortedDequeueWindowUniqueMultithreaded() throws Exception {
        StreamerBoundedSizeSortedWindow<Integer> win = new StreamerBoundedSizeSortedWindow<>();

        win.setMaximumSize(500);
        win.setUnique(true);

        win.start();

        checkWindowMultithreaded(win, 100000, 10, 1000);

        win.consistencyCheck();

        finalChecks(win, 500);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoundedTimeDequeueWindowMultithreaded() throws Exception {
        StreamerBoundedTimeWindow<Integer> win = new StreamerBoundedTimeWindow<>();

        win.setMaximumSize(500);
        win.setTimeInterval(40); // 40ms time interval.
        win.setUnique(false);

        win.start();

        checkWindowMultithreaded(win, 100000, 10, 1000);

        win.consistencyCheck();

        finalChecks(win, 500);

        U.sleep(1000);

        finalChecks(win, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoundedTimeDequeueWindowUniqueMultithreaded() throws Exception {
        StreamerBoundedTimeWindow<Integer> win = new StreamerBoundedTimeWindow<>();

        win.setMaximumSize(500);
        win.setTimeInterval(40); // 40ms time interval.
        win.setUnique(true);

        win.start();

        checkWindowMultithreaded(win, 100000, 10, 1000);

        win.consistencyCheck();

        finalChecks(win, 500);

        U.sleep(1000);

        finalChecks(win, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testBoundedTimeBatchDequeueWindowMultithreaded() throws Exception {
        StreamerBoundedTimeBatchWindow<Integer> win = new StreamerBoundedTimeBatchWindow<>();

        win.setMaximumBatches(10);
        win.setBatchTimeInterval(100);
        win.setBatchSize(50);

        win.start();

        checkWindowMultithreaded(win, 100000, 10, 1000);

        win.consistencyCheck();

        finalChecks(win, 500);

        U.sleep(1000);

        finalChecks(win, 0);
    }

    /**
     * Check iterator behaviour.
     *
     * @param win Window.
     * @throws Exception If failed.
     */
    private void checkIterator(StreamerWindow<Integer> win) throws Exception {
        win.reset();

        assert win.size() == 0;

        win.enqueue(1);

        assert win.size() == 1;

        final Iterator<Integer> iter = win.iterator();

        win.enqueue(2);

        assert win.size() == 2;

        assert iter.hasNext();

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                iter.remove();

                return null;
            }
        }, IllegalStateException.class, null);

        assert iter.next() == 1;

        iter.remove();

        assert !iter.hasNext();

        GridTestUtils.assertThrows(log(), new Callable<Object>() {
            @Override public Object call() throws Exception {
                iter.next();

                return null;
            }
        }, NoSuchElementException.class, null);

        assert win.size() == 1;
    }

    /**
     * Final checks.
     *
     * @param win Window to check.
     * @param maxSize Max window size.
     * @throws GridException If failed.
     */
    private void finalChecks(StreamerWindow<Integer> win, int maxSize) throws GridException {
        int evictQueueSize = win.evictionQueueSize();

        info("Eviction queue size for final checks: " + evictQueueSize);

        Collection<Integer> evicted = win.pollEvictedAll();

        info("Evicted entries in final checks: " + evicted.size());

        int winSize = win.size();

        win.pollEvictedAll();

        assertTrue("Unexpected window size [winSize=" + winSize + " maxSize=" + maxSize + ']', winSize <= maxSize);
    }

    /**
     * @param win Window to check.
     * @param iterCnt Iteration count.
     * @param threadCnt Thread count.
     * @param range Range for key generation.
     * @throws Exception If failed.
     */
    private void checkWindowMultithreaded(
        final StreamerWindow<Integer> win,
        final int iterCnt,
        int threadCnt,
        final int range
    ) throws Exception {
        final AtomicInteger polled = new GridAtomicInteger();

        final AtomicInteger added = new GridAtomicInteger();

        IgniteFuture<?> fut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                Random rnd = new Random();

                for (int i = 0; i < iterCnt; i++) {
                    if (i > 0 && i % 10000 == 0)
                        info("Finished " + i + " iterations");

                    int op = rnd.nextInt(8);

                    switch (op) {
                        case 0: {
                            // Add.
                            for (int j = 0; j < 30; j++)
                                win.enqueue(rnd.nextInt(range));

                            added.addAndGet(30);

                            break;
                        }

                        case 1: {
                            // Add bunch.
                            for (int j = 0; j < 10; j++)
                                win.enqueue(rnd.nextInt(range), rnd.nextInt(range), rnd.nextInt(range),
                                    rnd.nextInt(range), rnd.nextInt(range), rnd.nextInt(range));

                            added.addAndGet(10 * 6);

                            break;
                        }

                        case 2: {
                            Object o = win.pollEvicted();

                            if (o != null)
                                polled.incrementAndGet();

                            break;
                        }

                        case 3: {
                            Collection<Integer> p0 = win.pollEvicted(50);

                            polled.addAndGet(p0.size());

                            break;
                        }

                        case 4: {
                            Collection<Integer> p0 = win.pollEvictedBatch();

                            polled.addAndGet(p0.size());

                            break;
                        }

                        case 5: {
                            Object o = win.dequeue();

                            if (o != null)
                                polled.incrementAndGet();

                            break;
                        }

                        case 6: {
                            Collection<Integer> p0 = win.dequeue(50);

                            polled.addAndGet(p0.size());

                            break;
                        }

                        case 7: {
                            Iterator<Integer> it = win.iterator();

                            while (it.hasNext()) {
                                it.next();

                                if (rnd.nextInt(10) == 5) {
                                    it.remove();

                                    polled.incrementAndGet();
                                }
                            }

                            break;
                        }
                    }
                }

                return null;
            }
        }, threadCnt);

        fut.get();

        // Cannot assert on added, polled and window size because iterator does not return status.
        info("Window size: " + win.size());
        info("Added=" + added.get() + ", polled=" + polled.get());
    }

    /**
     * Check snapshto content.
     *
     * @param snapshot Snapshot.
     * @param vals Expected values.
     */
    private void checkSnapshot(Collection<Integer> snapshot, Object... vals) {
        assert snapshot.size() == vals.length;

        int i = 0;

        for (Object evt : snapshot)
            assertTrue(F.eq(evt, vals[i++]));
    }
}
