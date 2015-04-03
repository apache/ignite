/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.distributed.dht;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.testframework.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
@SuppressWarnings("unchecked")
public class IgniteCacheLockFailoverSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGridsMultiThreaded(gridCount());
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(gridName);

        ccfg.setNearConfiguration(null);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 2 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    public void testLockFailover() throws Exception {
        IgniteCache<Integer, Integer> cache = grid(0).cache(null);

        Integer key = backupKey(cache);

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> restartFut = GridTestUtils.runAsync(new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                while (!stop.get()) {
                    stopGrid(1);

                    U.sleep(500);

                    startGrid(1);
                }
                return null;
            }
        });

        try {
            long end = System.currentTimeMillis() + 60_000;

            long iter = 0;

            while (System.currentTimeMillis() < end) {
                if (iter % 100 == 0)
                    log.info("Iteration: " + iter);

                iter++;

                GridCacheAdapter<Object, Object> adapter = ((IgniteKernal)grid(0)).internalCache(null);

                IgniteInternalFuture<Boolean> fut = adapter.lockAsync(key, 0);

                try {
                    fut.get(30_000);

                    U.sleep(1);
                }
                catch (IgniteFutureTimeoutException e) {
                    info("Entry: " + adapter.peekEx(key));

                    fail("Lock timeout [fut=" + fut + ", err=" + e + ']');
                }
                catch (Exception e) {
                    log.error("Error: " + e);
                }
                finally {
                    adapter.unlock(key);
                }
            }
        }
        finally {
            stop.set(true);

            restartFut.get();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testUnlockPrimaryLeft() throws Exception {
        GridCacheAdapter<Integer, Integer> cache = ((IgniteKernal)grid(0)).internalCache(null);

        Integer key = backupKey(grid(0).cache(null));

        cache.lock(key, 0);

        stopGrid(1);

        cache.unlock(key);

        GridCacheEntryEx entry = cache.peekEx(key);

        assertTrue("Remote MVCC is not empty: " + entry, entry == null || entry.remoteMvccSnapshot().isEmpty());

        startGrid(1);
    }
}
