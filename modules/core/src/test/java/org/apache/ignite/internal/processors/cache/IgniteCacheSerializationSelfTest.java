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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgniteCacheSerializationSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES = 3;

    /** */
    private static final int CLIENT = NODES - 1;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODES - 1);
        startClientGrid(CLIENT);
    }

    /**
     * @param cacheMode Cache mode.
     * @param atomicityMode Atomicity mode.
     * @return Cache configuration.
     */
    private CacheConfiguration<Integer, Integer> cacheConfiguration(CacheMode cacheMode, CacheAtomicityMode atomicityMode) {
        CacheConfiguration<Integer, Integer> ccfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(cacheMode);
        ccfg.setAtomicityMode(atomicityMode);

        ccfg.setBackups(1);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSerializeClosure() throws Exception {
        Ignite client = ignite(CLIENT);

        final IgniteCache<Integer, Integer> clientCache = client.createCache(cacheConfiguration(PARTITIONED, ATOMIC));

        try {
            client.compute(client.cluster().forRemotes().forRandom()).call(new IgniteCallable<Object>() {
                @Override public Object call() throws Exception {
                    clientCache.withKeepBinary();
                    clientCache.withSkipStore();

                    return null;
                }
            });
        }
        finally {
            client.destroyCache(DEFAULT_CACHE_NAME);
        }
    }
}
