package org.apache.ignite.internal.processors.cache.datastructures;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCondition;
import org.apache.ignite.IgniteLock;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.testframework.GridTestUtils;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class IgniteReentrantLockTest extends IgniteAtomicsAbstractTest {
    /** */
    private static final int NODES_CNT = 4;

    /** */
    protected static final int THREADS_CNT = 5;

    /** */
    private static final Random RND = new Random();

    /** {@inheritDoc} */
    @Override protected CacheMode atomicsCacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return NODES_CNT;
    }

    /**
     * @throws Exception If failed.
     */
    public void testInitialization() throws Exception {
        try (IgniteLock lock = createReentrantLock(0, "lock"+false, false)) {
        }

        try (IgniteLock lock = createReentrantLock(0, "lock"+true, true)) {
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLock() throws Exception {
        testReentrantLock(true);
        testReentrantLock(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLock(final boolean fair) throws Exception {
        for (int i = 0; i < gridCount(); i++) {
            IgniteLock lock = createReentrantLock(i,"lock"+fair, fair);

            assertFalse(lock.isLocked());

            lock.lock();

            assertTrue(lock.isLocked());

            lock.unlock();

            assertFalse(lock.isLocked());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinode() throws Exception {
        testReentrantLockMultinode(false);
        testReentrantLockMultinode(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinode(final boolean fair) throws Exception {
        List<IgniteInternalFuture<Void>> futs = new ArrayList<>();

        for (int i = 0; i < gridCount(); i++) {
            final Ignite ignite = grid(i);
            final int inx = i;

            futs.add(GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteLock lock = createReentrantLock(inx,"lock"+fair, fair);

                    lock.lock();

                    try {
                        assertTrue(lock.isLocked());
                        Thread.sleep(1_000L);
                    } finally {
                        lock.unlock();
                    }

                    return null;
                }
            }));
        }

        for (IgniteInternalFuture<?> fut : futs)
            fut.get(30_000L);
    }

    /**
     * @throws Exception If failed.
     */
//    public void testReentrantLockMultinodeFailoverUnfair() throws Exception {
//        testReentrantLockMultinodeFailover(false);
//        stopAllGrids();
//    }

    /**
     * @throws Exception If failed.
     */
//    public void testReentrantLockMultinodeFailoverFair() throws Exception {
//        testReentrantLockMultinodeFailover(true);
//    }

    /**
     * @throws Exception If failed.
     */
    public void testReentrantLockMultinodeFailover(final boolean fair) throws Exception {
        List<IgniteInternalFuture<Void>> futs = new ArrayList<>();

        for (int i = 0; i < gridCount(); i++) {
            final Ignite ignite = grid(i);
            final int inx = i;
            final boolean flag = RND.nextBoolean();
            futs.add(GridTestUtils.runAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    IgniteLock lock = createReentrantLock(inx,"lock"+fair, fair);
                    UUID id = grid(inx).cluster().localNode().id();

                    lock.lock();

                    if (flag) {
                        try {
                            ignite.close();
                        }
                        catch (Exception ignored) {
                            lock.unlock();
                        }

                        return null;
                    }

                    try {
                        assertTrue(lock.isLocked());
                        Thread.sleep(1_000L);
                    } finally {
                        lock.unlock();
                    }

                    return null;
                }
            }));
        }

        for (IgniteInternalFuture<?> fut : futs)
            fut.get(60_000L);
    }

    /**
     * @param lockName Reentrant lock name.
     * @param fair Fairness flag.
     * @return Distributed reentrant lock.
     * @throws Exception If failed.
     */
    private IgniteLock createReentrantLock(int cnt, String lockName, boolean fair) throws Exception {
        assert lockName != null;
        assert cnt >= 0;

        IgniteLock lock = grid(cnt).reentrantLock(lockName, fair, true);

        assertNotNull(lock);
        assertEquals(lockName, lock.name());
        assertTrue(lock.isFailoverSafe());
        assertEquals(lock.isFair(), fair);

        return lock;
    }
}
