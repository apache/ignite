/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;

import java.util.concurrent.atomic.*;

/**
 * Checks that number of concurrent asynchronous operations is limited when configuration parameter is set.
 */
public class GridCacheAsyncOperationsLimitSelfTest extends GridCacheAbstractSelfTest {
    /** */
    public static final int MAX_CONCURRENT_ASYNC_OPS = 50;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cCfg = super.cacheConfiguration(gridName);

        cCfg.setMaxConcurrentAsyncOperations(MAX_CONCURRENT_ASYNC_OPS);

        return cCfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testAsyncOps() throws Exception {
        final AtomicInteger cnt = new AtomicInteger();
        final GridAtomicInteger max = new GridAtomicInteger();

        for (int i = 0; i < 5000; i++) {
            final int i0 = i;

            cnt.incrementAndGet();

            IgniteFuture<Boolean> fut = cache().putxAsync("key" + i, i);

            fut.listenAsync(new CI1<IgniteFuture<Boolean>>() {
                @Override public void apply(IgniteFuture<Boolean> t) {
                    cnt.decrementAndGet();

                    max.setIfGreater(cnt.get());

                    if (i0 > 0 && i0 % 100 == 0)
                        info("cnt: " + cnt.get());
                }
            });

            assertTrue("Maximum number of permits exceeded: " + max.get(),  max.get() <= 51);
        }
    }
}
