/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.cache.query.annotations.QueryTextField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.query.CacheQuery;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

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
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

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

        final GridCacheAdapter<Integer, H2TextValue> c = ((IgniteKernal)grid(0)).internalCache(null);

        IgniteInternalFuture<?> fut1 = multithreadedAsync(new Callable() {
                @Override public Object call() throws Exception {
                    for (int i = 0; i < keyCnt; i++) {
                        c.getAndPut(i, new H2TextValue(txt));

                        if (i % logFreq == 0)
                            X.println("Stored values: " + i);
                    }

                    return null;
                }
            }, 1);

        // Create query.
        final CacheQuery<Map.Entry<Integer, H2TextValue>> qry = c.context().queries().createFullTextQuery(
            H2TextValue.class.getName(), txt, false);

        qry.enableDedup(false);
        qry.includeBackups(false);
        qry.timeout(TEST_TIMEOUT);

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut2 = multithreadedAsync(new Callable() {
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
        @QueryTextField
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