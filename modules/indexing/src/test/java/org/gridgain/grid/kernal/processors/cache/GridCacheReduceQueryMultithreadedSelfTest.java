/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.query.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.spi.indexing.h2.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Multithreaded reduce query tests with lots of data.
 */
public class GridCacheReduceQueryMultithreadedSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 5;

    /** */
    private static final int TEST_TIMEOUT = 2 * 60 * 1000;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridH2IndexingSpi indexing = new GridH2IndexingSpi();

        indexing.setDefaultIndexPrimitiveKey(true);

        c.setIndexingSpi(indexing);

        c.setMarshaller(new GridOptimizedMarshaller(false));

        return c;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheConfiguration cacheConfiguration(String gridName) throws Exception {
        GridCacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(1);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /**
     * @throws Exception In case of error.
     */
    public void testReduceQuery() throws Exception {
        final int keyCnt = 5000;
        final int logFreq = 500;

        final GridCache<String, Integer> c = cache();

        final CountDownLatch startLatch = new CountDownLatch(1);

        GridFuture<?> fut1 = multithreadedAsync(new Callable() {
            @Override public Object call() throws Exception {
                for (int i = 1; i < keyCnt; i++) {
                    assertTrue(c.putx(String.valueOf(i), i));

                    startLatch.countDown();

                    if (i % logFreq == 0)
                        info("Stored entries: " + i);
                }

                return null;
            }
        }, 1);

        // Create query.
        final GridCacheQuery<Map.Entry<String, Integer>> sumQry =
            c.queries().createSqlQuery(Integer.class, "_val > 0").timeout(TEST_TIMEOUT);

        final R1<Map.Entry<String, Integer>, Integer> rmtRdc = new R1<Map.Entry<String, Integer>, Integer>() {
            /** */
            private AtomicInteger sum = new AtomicInteger();

            @Override public boolean collect(Map.Entry<String, Integer> e) {
                sum.addAndGet(e.getValue());

                return true;
            }

            @Override public Integer reduce() {
                return sum.get();
            }
        };

        final AtomicBoolean stop = new AtomicBoolean();

        startLatch.await();

        GridFuture<?> fut2 = multithreadedAsync(new Callable() {
            @Override public Object call() throws Exception {
                int cnt = 0;

                while (!stop.get()) {
                    Collection<Integer> res = sumQry.execute(rmtRdc).get();

                    int sum = F.sumInt(res);

                    cnt++;

                    assertTrue(sum > 0);

                    if (cnt % logFreq == 0) {
                        info("Reduced value: " + sum);
                        info("Executed queries: " + cnt);
                    }
                }

                return null;
            }
        }, 1);

        fut1.get();

        stop.set(true);

        fut2.get();
    }
}
