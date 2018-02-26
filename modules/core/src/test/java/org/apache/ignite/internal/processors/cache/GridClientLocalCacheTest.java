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

import javax.cache.CacheException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.testframework.GridTestUtils.assertThrows;

/**
 * Test that absence of configured data region for client produces correct exception.
 */
public class GridClientLocalCacheTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_REGION = "test-region";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String name) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(name);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(LOCAL_IP_FINDER);

        cfg.setClientMode(name.contains("client"));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    public void testStatic() throws Exception {
        startGrid("server");

        final IgniteConfiguration cfg = getConfiguration("client")
            .setCacheConfiguration(new CacheConfiguration("test").setCacheMode(LOCAL));

        assertThrows(log, () -> {
            G.start(cfg);

            return null;
        }, IgniteException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStaticDataRegion() throws Exception {
        startGrid(getConfiguration("server").setDataStorageConfiguration(getDataStorageConfiguration()));

        final IgniteConfiguration cfg = getConfiguration("client")
            .setDataStorageConfiguration(getDataStorageConfiguration())
            .setCacheConfiguration(new CacheConfiguration("test").setCacheMode(LOCAL).setDataRegionName(TEST_REGION));

        G.start(cfg);
    }

    /**
     *
     */
    private DataStorageConfiguration getDataStorageConfiguration() {
        return new DataStorageConfiguration()
            .setDataRegionConfigurations(new DataRegionConfiguration().setName(TEST_REGION));
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ThrowableNotThrown")
    public void testDynamic() throws Exception {
        startGrid("server");
        final Ignite client = startGrid("client");

        assertThrows(log, () -> {
            client.createCache(new CacheConfiguration<>("test").setCacheMode(LOCAL));

            return null;
        }, CacheException.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testDynamicDataRegion() throws Exception {
        startGrid(getConfiguration("server").setDataStorageConfiguration(getDataStorageConfiguration()));

        final IgniteConfiguration cfg = getConfiguration("client")
            .setDataStorageConfiguration(getDataStorageConfiguration());

        IgniteEx client = startGrid(cfg);

        client.createCache(new CacheConfiguration<>("test").setCacheMode(LOCAL).setDataRegionName(TEST_REGION));
    }
}
