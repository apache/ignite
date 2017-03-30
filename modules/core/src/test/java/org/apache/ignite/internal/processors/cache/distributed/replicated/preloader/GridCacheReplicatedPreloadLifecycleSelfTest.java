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

package org.apache.ignite.internal.processors.cache.distributed.replicated.preloader;

import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CachePeekMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.GridCachePreloadLifecycleAbstractTest;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;

/**
 * Tests for replicated cache preloader.
 */
@SuppressWarnings({"PublicInnerClass"})
public class GridCacheReplicatedPreloadLifecycleSelfTest extends GridCachePreloadLifecycleAbstractTest {
    /** */
    private static boolean quiet = true;

    /** Grid count. */
    private int gridCnt = 5;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.getTransactionConfiguration().setDefaultTxConcurrency(OPTIMISTIC);
        c.getTransactionConfiguration().setDefaultTxIsolation(READ_COMMITTED);

        CacheConfiguration cc1 = defaultCacheConfiguration();

        cc1.setName("one");
        cc1.setCacheMode(REPLICATED);
        cc1.setWriteSynchronizationMode(FULL_SYNC);
        cc1.setRebalanceMode(preloadMode);
        cc1.setEvictionPolicy(null);
        cc1.setSwapEnabled(false);
        cc1.setCacheStoreFactory(null);

        // Identical configuration.
        CacheConfiguration cc2 = new CacheConfiguration(cc1);

        cc2.setName("two");

        c.setCacheConfiguration(cc1, cc2);

        return c;
    }

    /**
     * @param keys Keys.
     * @return Lifecycle bean.
     */
    private LifecycleBean lifecycleBean(final Object[] keys) {
        return new LifecycleBean() {
            @IgniteInstanceResource
            private Ignite ignite;

            @Override public void onLifecycleEvent(LifecycleEventType evt) {
                switch (evt) {
                    case AFTER_NODE_START: {
                        IgniteCache<Object, MyValue> c1 = ignite.cache("one");
                        IgniteCache<Object, MyValue> c2 = ignite.cache("two");

                        if (!ignite.name().contains("Test0")) {
                            if (!quiet) {
                                info("Keys already in cache:");

                                for (Cache.Entry<Object, MyValue> entry : c1)
                                    info("Cache1: " + entry.getKey().toString());

                                for (Cache.Entry<Object, MyValue> entry : c2)
                                    info("Cache2: " + entry.getKey().toString());
                            }

                            return;
                        }

                        info("Populating cache data...");

                        int i = 0;

                        for (Object key : keys) {
                            c1.put(key, new MyValue(value(key)));

                            if (i++ % 2 == 0)
                                c2.put(key, new MyValue(value(key)));
                        }

                        assert c1.size() == keys.length : "Invalid cache1 size [size=" + c1.size() + ']';
                        assert c2.size() == keys.length / 2 : "Invalid cache2 size [size=" + c2.size() + ']';

                        break;
                    }

                    case BEFORE_NODE_START:
                    case BEFORE_NODE_STOP:
                    case AFTER_NODE_STOP: {
                        info("Lifecycle event: " + evt);

                        break;
                    }
                }
            }
        };
    }

    /**
     * @param keys Keys.
     * @throws Exception If failed.
     */
    public void checkCache(Object[] keys) throws Exception {
        preloadMode = SYNC;

        lifecycleBean = lifecycleBean(keys);

        for (int i = 0; i < gridCnt; i++) {
            startGrid(i);

            info("Checking '" + (i + 1) + "' nodes...");

            for (int j = 0; j < G.allGrids().size(); j++) {
                IgniteCache<String, MyValue> c1 = grid(j).cache("one");
                IgniteCache<String, MyValue> c2 = grid(j).cache("two");

                int size1 = c1.localSize(CachePeekMode.ALL);
                int size2 = c2.localSize(CachePeekMode.ALL);

                assertEquals(" Invalid cache1 size [i=" + i + ", j=" + j + ", size=" + size1 + ']', keys.length, size1);
                assertEquals(" Invalid cache2 size [i=" + i + ", j=" + j + ", size=" + size2 + ']', keys.length / 2, size2);
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testLifecycleBean1() throws Exception {
        checkCache(keys(true, DFLT_KEYS.length, DFLT_KEYS));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLifecycleBean2() throws Exception {
        checkCache(keys(false, DFLT_KEYS.length, DFLT_KEYS));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLifecycleBean3() throws Exception {
        checkCache(keys(true, 500));
    }

    /**
     * @throws Exception If failed.
     */
    public void testLifecycleBean4() throws Exception {
        checkCache(keys(false, 500));
    }
}
