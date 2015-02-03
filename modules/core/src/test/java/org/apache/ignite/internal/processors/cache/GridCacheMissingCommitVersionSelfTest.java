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

import org.apache.ignite.cache.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;

/**
 *
 */
public class GridCacheMissingCommitVersionSelfTest extends GridCommonAbstractTest {
    /** */
    private volatile Integer failedKey;

    /** */
    private String maxCompletedTxCount;

    /**
     */
    public GridCacheMissingCommitVersionSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        maxCompletedTxCount = System.getProperty(IGNITE_MAX_COMPLETED_TX_COUNT);

        System.setProperty(IGNITE_MAX_COMPLETED_TX_COUNT, String.valueOf(5));

        IgniteConfiguration cfg = super.getConfiguration();

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);

        CacheConfiguration ccfg = new CacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        System.setProperty(IGNITE_MAX_COMPLETED_TX_COUNT, maxCompletedTxCount != null ? maxCompletedTxCount : "");

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMissingCommitVersion() throws Exception {
        final GridCache<Integer, Integer> cache = cache();

        final int KEYS_PER_THREAD = 10_000;

        final AtomicInteger keyStart = new AtomicInteger();

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int start = keyStart.getAndAdd(KEYS_PER_THREAD);

                for (int i = 0; i < KEYS_PER_THREAD && failedKey == null; i++) {
                    int key = start + i;

                    try {
                        cache.put(key, 1);
                    }
                    catch (Exception e) {
                        log.info("Put failed: " + e);

                        failedKey = key;
                    }
                }


                return null;
            }
        }, 10, "put-thread");

        assertNotNull("Test failed to provoke 'missing commit version' error.", failedKey);

        log.info("Trying to update " + failedKey);

        IgniteInternalFuture<?> fut = cache.putAsync(failedKey, 2);

        try {
            fut.get(5000);
        }
        catch (IgniteFutureTimeoutCheckedException ignore) {
            fail("Put failed to finish in 5s.");
        }
    }
}
