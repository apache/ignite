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

import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.store.CacheStore;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Tests the cache creation which takes more time than {@link IgniteConfiguration#failureDetectionTimeout}.
 */
public class LongRunningCacheCreationTest extends GridCommonAbstractTest {
    /** */
    private static final long TEST_FAILURE_TIMEOUT = 15_000L;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureDetectionTimeout(TEST_FAILURE_TIMEOUT);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected FailureHandler getFailureHandler(String igniteInstanceName) {
        return new StopNodeFailureHandler();
    }

    /**
     * @throws Exception In case of an error.
     */
    @SuppressWarnings("unchecked")
    public void testCacheCreation() throws Exception {
        try {
            startGrid(0);

            startGrid(1);

            Ignite client = startGrid(getConfiguration("client").setClientMode(true));

            assertEquals(3, client.cluster().topologyVersion());

            CacheConfiguration ccfg = new CacheConfiguration("testCache");

            ccfg.setCacheStoreFactory((Factory<CacheStore>)() -> {
                try {
                    U.sleep(TEST_FAILURE_TIMEOUT * 2);
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    // No-op.
                }

                return null;
            });

            IgniteCache cache = client.getOrCreateCache(ccfg);

            assertEquals(3, client.cluster().topologyVersion());

            assertNotNull(cache);
        }
        finally {
            stopAllGrids();
        }
    }
}
