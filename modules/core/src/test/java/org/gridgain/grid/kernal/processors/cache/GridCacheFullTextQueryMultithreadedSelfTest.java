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
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Multithreaded reduce query tests with lots of data.
 */
public class GridCacheFullTextQueryMultithreadedSelfTest extends GridCacheAbstractSelfTest {
    /** */
    private static final int GRID_CNT = 3;

    /** */
    private static final int TEST_TIMEOUT = 15 * 60 * 1000;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return TEST_TIMEOUT;
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
     * JUnit.
     *
     * @throws Exception In case of error.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testH2Text() throws Exception {
        int duration = 60 * 1000;
        final int keyCnt = 5000;
        final int logFreq = 50;
        final String txt = "Value";

        final GridCache<Integer, H2TextValue> c = grid(0).cache(null);

        IgniteFuture<?> fut1 = multithreadedAsync(new Callable() {
                @Override public Object call() throws Exception {
                    for (int i = 0; i < keyCnt; i++) {
                        c.putx(i, new H2TextValue(txt));

                        if (i % logFreq == 0)
                            X.println("Stored values: " + i);
                    }

                    return null;
                }
            }, 1);

        // Create query.
        final GridCacheQuery<Map.Entry<Integer, H2TextValue>> qry = c.queries().createFullTextQuery(
            H2TextValue.class, txt);

        qry.enableDedup(false);
        qry.includeBackups(false);
        qry.timeout(TEST_TIMEOUT);

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteFuture<?> fut2 = multithreadedAsync(new Callable() {
                @Override public Object call() throws Exception {
                    int cnt = 0;

                    while (!stop.get()) {
                        Collection<Map.Entry<Integer, H2TextValue>> res = qry.execute().get();

                        cnt++;

                        if (cnt % logFreq == 0) {
                            X.println("Result set: " + res.size());
                            X.println("Executed queries: " + cnt);
                        }
                    }

                    return null;
                }
            }, 1);

        Thread.sleep(duration);

        fut1.get();

        stop.set(true);

        fut2.get();
    }

    /**
     *
     */
    private static class H2TextValue {
        /** */
        @GridCacheQueryTextField
        private final String val;

        /**
         * @param val String value.
         */
        H2TextValue(String val) {
            this.val = val;
        }

        /**
         * @return String field value.
         */
        String value() {
            return val;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(H2TextValue.class, this);
        }
    }
}
