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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import javax.cache.configuration.*;

/**
 * Test resource injection.
 */
public class StoreResourceInjectionSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private CacheConfiguration<Integer, String> cacheCfg = new CacheConfiguration<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cacheCfg.setReadThrough(true);
        cacheCfg.setWriteThrough(true);

        cfg.setCacheConfiguration(cacheCfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     *
     */
    public void testResourcesInStoreFactory() throws Exception {
        cacheCfg.setCacheStoreFactory(new MyCacheStoreFactory());

        startGrid(0);
    }

    /**
     *
     */
    public void testResourcesInLoaderFactory() throws Exception {
        cacheCfg.setCacheLoaderFactory(new MyCacheStoreFactory());

        startGrid(0);
    }

    /**
     *
     */
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
