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

package org.apache.ignite;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.binary.BinaryMarshaller;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.IgniteCacheAbstractTest;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 */
public class TestClosureTmp extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new BinaryMarshaller());

//        cfg.setCacheConfiguration(cacheConfig());

        return cfg;
    }

    private CacheConfiguration cacheConfig() {
        CacheConfiguration config = new CacheConfiguration("persistent-cache");
        config.setCacheMode(CacheMode.REPLICATED);
        config.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        config.setRebalanceMode(CacheRebalanceMode.SYNC);
        config.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        config.setStartSize(1024);
        config.setCacheStoreFactory(new IgniteCacheAbstractTest.TestStoreFactory());
        config.setWriteThrough(true);
        return config;
    }

    /**
     * @throws Exception If failed.
     */
    public void testName() throws Exception {
        IgniteEx ignite1 = startGrid(0);

        IgniteCache cache = ignite1.getOrCreateCache(cacheConfig());

        CountDownLatch gridStartedLatch = new CountDownLatch(1);
        CountDownLatch gotExceptionLatch = new CountDownLatch(1);

        GridCacheProcessor.gridStartedLatchStatic = gridStartedLatch;
        GridCacheProcessor.gotExceptionLatchStatic = gotExceptionLatch;

        IgniteInternalFuture<Long> future = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                startGrid(1);

                return null;
            }
        }, 1, "grid-2-starter");

        gridStartedLatch.await();

        try {
            cache.loadCache(null);
        }
        catch (Throwable e) {
            e.printStackTrace();

            gotExceptionLatch.countDown();

            fail("Got exception: " + e);
        }
    }
}
