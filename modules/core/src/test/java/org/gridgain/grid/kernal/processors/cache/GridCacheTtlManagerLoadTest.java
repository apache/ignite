/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.typedef.internal.*;

import javax.cache.expiry.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Check ttl manager for memory leak.
 */
public class GridCacheTtlManagerLoadTest extends GridCacheTtlManagerSelfTest {
    /**
     * @throws Exception If failed.
     */
    public void testLoad() throws Exception {
        cacheMode = REPLICATED;

        final GridKernal g = (GridKernal)startGrid(0);

        try {
            final AtomicBoolean stop = new AtomicBoolean();

            IgniteFuture<?> fut = multithreadedAsync(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    IgniteCache<Object,Object> cache = g.jcache(null).
                        withExpiryPolicy(new TouchedExpiryPolicy(new Duration(MILLISECONDS, 1000)));

                    long key = 0;

                    while (!stop.get()) {
                        cache.put(key, key);

                        key++;
                    }

                    return null;
                }
            }, 1);

            GridCacheTtlManager<Object, Object> ttlMgr = g.internalCache().context().ttl();

            for (int i = 0; i < 300; i++) {
                U.sleep(1000);

                ttlMgr.printMemoryStats();
            }

            stop.set(true);

            fut.get();
        }
        finally {
            stopAllGrids();
        }
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return Long.MAX_VALUE;
    }
}
