/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.near;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.processors.cache.*;

import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Test for asynchronous cache entry lock with timeout.
 */
public class GridCachePartitionedEntryLockSelfTest extends GridCacheAbstractSelfTest {
    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("BusyWait")
    public void testLockAsyncWithTimeout() throws Exception {
        cache().put("key", 1);

        for (int i = 0; i < gridCount(); i++) {
            final GridCacheEntry<String, Integer> e = cache(i).entry("key");

            if (e.backup()) {
                assert !e.isLocked();

                e.lockAsync(2000).get();

                assert e.isLocked();

                GridCompute comp = grid(i).forLocal().compute().enableAsync();

                comp.call(new Callable<Boolean>() {
                    @Override public Boolean call() throws Exception {
                        GridFuture<Boolean> f = e.lockAsync(1000);

                        try {
                            f.get(100);

                            fail();
                        }
                        catch (GridFutureTimeoutException ex) {
                            info("Caught expected exception: " + ex);
                        }

                        try {
                            assert f.get();
                        }
                        finally {
                            e.unlock();
                        }

                        return true;
                    }
                });

                GridFuture<Boolean> f = comp.future();

                // Let another thread start.
                Thread.sleep(300);

                assert e.isLocked();
                assert e.isLockedByThread();

                cache().unlock("key");

                assert f.get();

                for (int j = 0; j < 100; j++)
                    if (cache().isLocked("key") || cache().isLockedByThread("key"))
                        Thread.sleep(10);
                    else
                        break;

                assert !cache().isLocked("key");
                assert !cache().isLockedByThread("key");

                break;
            }
        }
    }
}
