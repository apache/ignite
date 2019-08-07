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

package org.apache.ignite.cache.store;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CacheWriterException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test checks that local cacheLoad task never blocks remote
 * cacheLoad.
 */
public class GridStoreLoadCacheTest extends GridCommonAbstractTest {
    /** Barrier. */
    private static final CyclicBarrier BARRIER = new CyclicBarrier(3);

    /** Cache name. */
    public static final String CACHE_NAME = "test";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        //noinspection unchecked
        cfg.setCacheConfiguration(new CacheConfiguration(CACHE_NAME).setCacheStoreFactory(new TestFactory()));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test() throws Exception {
        for (int i = 0; i < 3; i++) {
            IgniteEx srv1 = startGrid(0);
            startGrid(1);
            startGrid(2);

            awaitPartitionMapExchange();

            srv1.cache(CACHE_NAME).loadCache(null);

            stopAllGrids();
        }
    }

    /**
     *
     */
    private static class TestFactory implements Factory<CacheStore> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /** {@inheritDoc} */
        @Override public CacheStore create() {
            return new TestStore();
        }
    }

    /**
     *
     */
    private static class TestStore extends CacheStoreAdapter<Object, Object> {
        /** {@inheritDoc} */
        @Override public void loadCache(IgniteBiInClosure<Object, Object> clo, Object... args) {
            try {
                BARRIER.await(3, TimeUnit.SECONDS);
            }
            catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public Object load(Object key) throws CacheLoaderException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public void write(Cache.Entry<?, ?> entry) throws CacheWriterException {
            // No-op
        }

        /** {@inheritDoc} */
        @Override public void delete(Object key) throws CacheWriterException {
            // No-op
        }
    }
}
