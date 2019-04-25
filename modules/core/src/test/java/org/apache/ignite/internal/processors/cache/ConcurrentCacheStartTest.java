/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class ConcurrentCacheStartTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        try {
            final IgniteEx ignite = (IgniteEx) startGrids(4);

            for (int k = 0; k < 100; k++) {
                final String cacheName = "cache" + k;

                GridTestUtils.runMultiThreaded(new Runnable() {
                    @Override public void run() {
                        try {
                            ignite.context().cache().dynamicStartCache(
                                new CacheConfiguration().setName(cacheName),
                                cacheName,
                                null,
                                false,
                                false,
                                false
                            ).get();

                            assertNotNull(ignite.context().cache().cache(cacheName));
                        }
                        catch (IgniteCheckedException ex) {
                            throw new IgniteException(ex);
                        }
                    }
                }, 10, "cache-start");
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
