package org.apache.ignite.cache.store;/*
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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.testframework.junits.common.*;

import javax.cache.configuration.*;
import org.junit.Test;

/**
 * Test resource injection.
 */
public class StoreResourceInjectionSelfTest extends GridCommonAbstractTest {
    /** */
    private CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<>(DEFAULT_CACHE_NAME);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    @Test
    public void testResourcesInStoreFactory() throws Exception {
        cacheCfg.setCacheStoreFactory(new MyCacheStoreFactory());

        startGrid(0);
    }

    /**
     *
     */
    @Test
    public void testResourcesInLoaderFactory() throws Exception {
        cacheCfg.setCacheLoaderFactory(new MyCacheStoreFactory());

        startGrid(0);
    }

    /**
     *
     */
    @Test
    public void testResourcesInWriterFactory() throws Exception {
        cacheCfg.setCacheWriterFactory(new MyCacheStoreFactory());

        startGrid(0);
    }

    /**
     *
     */
    public static class MyCacheStoreFactory implements Factory<CacheStore<Integer, String>> {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public CacheStore<Integer, String> create() {
            assert ignite != null;

            return new GridCacheTestStore();
        }
    }
}
