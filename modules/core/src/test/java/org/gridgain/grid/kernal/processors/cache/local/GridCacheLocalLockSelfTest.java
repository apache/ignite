/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.local;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Test cases for multi-threaded tests.
 */
@SuppressWarnings({"ProhibitedExceptionThrown"})
public class GridCacheLocalLockSelfTest extends GridCommonAbstractTest {
    /** Grid. */
    private Ignite ignite;

    /**
     *
     */
    public GridCacheLocalLockSelfTest() {
        super(true /*start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        ignite = grid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        ignite = null;
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration() throws Exception {
        GridConfiguration cfg = super.getConfiguration();

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(disco);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(LOCAL);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @throws GridException If test failed.
     */
    public void testLockReentry() throws GridException {
        GridCache<Integer, String> cache = ignite.cache(null);

        assert !cache.isLocked(1);
        assert !cache.isLockedByThread(1);

        assert cache.lock(1, 0L);

        assert cache.isLocked(1);
        assert cache.isLockedByThread(1);

        try {
            assert cache.get(1) == null;
            assert cache.put(1, "1") == null;
            assert "1".equals(cache.get(1));

            // Reentry.
            assert cache.lock(1, 0L);

            assert cache.isLocked(1);
            assert cache.isLockedByThread(1);

            try {
                assert "1".equals(cache.remove(1));
            }
            finally {
                cache.unlock(1);
            }

            assert cache.isLocked(1);
            assert cache.isLockedByThread(1);
        }
        finally {
            cache.unlock(1);
        }

        assert !cache.isLocked(1);
        assert !cache.isLockedByThread(1);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testLock() throws Throwable {
        final GridCache<Integer, String> cache = ignite.cache(null);

        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        final CountDownLatch latch3 = new CountDownLatch(1);

        GridTestThread t1 = new GridTestThread(new Callable<Object>() {
            @SuppressWarnings({"CatchGenericClass"})
            @Nullable @Override public Object call() throws Exception {
                info("Before lock for.key 1");

                assert cache.lock(1, 0L);

                info("After lock for key 1");

                try {
                    assert cache.isLocked(1);
                    assert cache.isLockedByThread(1);

                    latch1.countDown();

                    info("Let thread2 proceed.");

                    cache.put(1, "1");

                    info("Put 1='1' key pair into cache.");

                    latch2.await();

                    info("Waited for latch 2");
                }
                finally {
                    cache.unlock(1);

                    info("Unlocked entry for key 1.");

                    latch3.countDown();
                }

                return null;
            }
        });

        GridTestThread t2 = new GridTestThread(new Callable<Object>() {
            @SuppressWarnings({"CatchGenericClass"})
            @Nullable @Override public Object call() throws Exception {
                info("Waiting for latch1...");

                latch1.await();

                info("Latch1 released.");

                assert !cache.lock(1, -1L);

                assert cache.isLocked(1);
                assert !cache.isLockedByThread(1);

                info("Tried to lock cache for key1");

                latch2.countDown();

                info("Released latch2");

                latch3.await();

                assert cache.lock(1, -1L);

                assert cache.isLocked(1);
                assert cache.isLockedByThread(1);

                try {
                    info("Locked cache for key 1");

                    assert "1".equals(cache.get(1));

                    info("Read value for key 1");

                    assert "1".equals(cache.remove(1));

                    info("Removed value for key 1");

                    assert cache.isLocked(1);
                    assert cache.isLockedByThread(1);

                    info("Checked that cache is locked for key 1");
                }
                finally {
                    cache.unlock(1);

                    info("Unlocked cache for key 1");
                }

                assert !cache.isLocked(1);
                assert !cache.isLockedByThread(1);

                return null;
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        t1.checkError();
        t2.checkError();

        assert !cache.isLockedByThread(1);
        assert !cache.isLocked(1);
    }

    /**
     * @throws Exception If test failed.
     */
    public void testLockAndPut() throws Throwable {
        final GridCache<Integer, String> cache = ignite.cache(null);

        final CountDownLatch latch1 = new CountDownLatch(1);

        GridTestThread t1 = new GridTestThread(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                assert cache.lock(1, 0L);

                info("Locked cache key: 1");

                try {
                    assert cache.isLockedByThread(1);
                    assert cache.isLocked(1);

                    info("Verified that cache key is locked: 1");

                    cache.put(1, "1");

                    info("Put key value pair into cache: 1='1'");

                    latch1.countDown();

                    info("Released latch1");

                    // Hold lock for a bit.
                    Thread.sleep(50);

                    info("Woke up from sleep.");
                }
                finally {
                    cache.unlock(1);

                    info("Unlocked cache key: 1");
                }

                return null;
            }
        });

        GridTestThread t2 = new GridTestThread(new Callable<Object>() {
            @Nullable @Override public Object call() throws Exception {
                info("Beginning to await on latch 1");

                latch1.await();

                info("Finished awaiting on latch 1");

                assert "1".equals(cache.get(1));

                info("Retrieved value from cache for key: 1");

                cache.put(1, "2");

                info("Put key-value pair into cache: 1='2'");

                assert "2".equals(cache.remove(1));

                info("Removed key from cache: 1");

                return null;
            }
        });

        t1.start();
        t2.start();

        t1.join();
        t2.join();

        t1.checkError();
        t2.checkError();

        assert !cache.isLockedByThread(1);
        assert !cache.isLocked(1);
    }
}
