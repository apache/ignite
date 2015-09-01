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
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public class IgniteCacheConfigurationDefaultTemplateTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        CacheConfiguration templateCfg = new CacheConfiguration();

        templateCfg.setName("org.apache.ignite.template*");
        templateCfg.setBackups(3);

        cfg.setCacheConfiguration(templateCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testDefaultTemplate() throws Exception {
        Ignite ignite = startGrid(0);

        checkDefaultTemplate(ignite, "org.apache.ignite");

        checkDefaultTemplate(ignite, "org.apache.ignite.templat");

        checkDefaultTemplate(ignite, null);

        checkGetOrCreate(ignite, "org.apache.ignite.template", 3);

        CacheConfiguration templateCfg = new CacheConfiguration();

        templateCfg.setBackups(4);

        ignite.addCacheConfiguration(templateCfg);

        checkGetOrCreate(ignite, "org.apache.ignite2", 4);
    }

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    private void checkDefaultTemplate(final Ignite ignite, final String cacheName) {
        checkGetOrCreate(ignite, cacheName, 0);

        IgniteCache cache = ignite.getOrCreateCache(cacheName);

        assertNotNull(cache);

        CacheConfiguration cfg = (CacheConfiguration)cache.getConfiguration(CacheConfiguration.class);

        assertEquals(CacheConfiguration.DFLT_CACHE_ATOMICITY_MODE, cfg.getAtomicityMode());
    }

    /**
     * @param ignite Ignite.
     * @param name Cache name.
     * @param expBackups Expected number of backups.
     */
    private void checkGetOrCreate(Ignite ignite, String name, int expBackups) {
        IgniteCache cache = ignite.getOrCreateCache(name);

        assertNotNull(cache);

        CacheConfiguration cfg = (CacheConfiguration)cache.getConfiguration(CacheConfiguration.class);

        assertEquals(name, cfg.getName());
        assertEquals(expBackups, cfg.getBackups());
    }
}