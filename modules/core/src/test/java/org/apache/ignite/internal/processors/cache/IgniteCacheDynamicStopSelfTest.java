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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.cache.Cache;
import javax.cache.CacheException;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteCacheDynamicStopSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(4);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopStartCacheWithDataLoaderNoOverwrite() throws Exception {
        checkStopStartCacheWithDataLoader(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopStartCacheWithDataLoaderOverwrite() throws Exception {
        checkStopStartCacheWithDataLoader(true);
    }

    /**
     * @param allowOverwrite Allow overwrite flag for streamer.
     * @throws Exception If failed.
     */
    public void checkStopStartCacheWithDataLoader(final boolean allowOverwrite) throws Exception {
        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(CacheMode.PARTITIONED);

        ignite(0).createCache(ccfg);

        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<Object> fut = GridTestUtils.runAsync(new Callable<Object>() {
            /** {@inheritDoc} */
            @Override public Object call() throws Exception {
                try (IgniteDataStreamer<Integer, Integer> str = ignite(0).dataStreamer(null)) {
                    str.allowOverwrite(allowOverwrite);

                    int i = 0;

                    while (!stop.get()) {
                        str.addData(i % 10_000, i).listen(new CI1<IgniteFuture<?>>() {
                            @Override public void apply(IgniteFuture<?> f) {
                                try {
                                    f.get();
                                }
                                catch (CacheException ignore) {
                                    // This may be debugged.
                                }
                            }
                        });

                        if (i > 0 && i % 10000 == 0)
                            info("Added: " + i);

                        i++;
                    }
                }

                return null;
            }
        });

        try {
            Thread.sleep(500);

            ignite(0).destroyCache(null);

            Thread.sleep(500);

            ignite(0).createCache(ccfg);
        }
        finally {
            stop.set(true);
        }

        fut.get();

        int cnt = 0;

        for (Cache.Entry<Object, Object> ignored : ignite(0).cache(null))
            cnt++;

        info(">>> cnt=" + cnt);

        ignite(0).destroyCache(null);
    }
}