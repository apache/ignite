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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/**
 * Test ensures that the put operation does not hang during asynchronous cache destroy.
 */
public class IgniteCachePutOnDestroyTest extends GridCommonAbstractTest {
    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    private <K, V> CacheConfiguration<K, V> cacheConfiguration(String cacheName, String grpName) {
        CacheConfiguration<K, V> cfg = new CacheConfiguration<>();

        cfg.setName(cacheName);
        cfg.setGroupName(grpName);
        cfg.setAtomicityMode(atomicityMode());

        return cfg;
    }

    /**
     * @return Cache atomicity mode.
     */
    public CacheAtomicityMode atomicityMode() {
        return TRANSACTIONAL;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutOnCacheDestroy() throws Exception {
        for (int n = 0; n < 50; n++)
            doTestPutOnCacheDestroy();
    }

    /**
     * @throws Exception If failed.
     */
    private void doTestPutOnCacheDestroy() throws Exception {
        final Ignite ignite = grid(0);

        final String grpName = "testGroup";

        IgniteCache additionalCache = ignite.createCache(cacheConfiguration("cache1", grpName));

        try {
            IgniteCache<Integer, Boolean> cache = ignite.getOrCreateCache(cacheConfiguration("cache2", grpName));

            AtomicInteger cntr = new AtomicInteger();

            GridTestUtils.runMultiThreadedAsync(() -> {
                try {
                    int key;

                    while ((key = cntr.getAndIncrement()) < 2_000) {
                        if (key == 1_000) {
                            cache.destroy();

                            break;
                        }

                        cache.put(key, true);
                    }
                }
                catch (Exception e) {
                    boolean rightErrMsg = false;

                    for (Throwable t : X.getThrowableList(e)) {
                        if (t.getClass() == CacheStoppedException.class ||
                            t.getClass() == IllegalStateException.class) {
                            String errMsg = t.getMessage().toLowerCase();

                            if (errMsg.contains("cache") && errMsg.contains("stopped")) {
                                rightErrMsg = true;

                                break;
                            }
                        }
                    }

                    assertTrue(X.getFullStackTrace(e), rightErrMsg);
                }

                return null;
            }, 6, "put-thread").get();
        }
        finally {
            additionalCache.destroy();
        }
    }
}
