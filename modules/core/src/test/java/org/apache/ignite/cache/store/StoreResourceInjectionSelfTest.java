package org.apache.ignite.cache.store;/*
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

import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheTestStore;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
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
