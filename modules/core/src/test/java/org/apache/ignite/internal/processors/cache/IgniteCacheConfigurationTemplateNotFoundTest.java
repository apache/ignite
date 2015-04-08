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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.concurrent.*;

/**
 *
 */
public class IgniteCacheConfigurationTemplateNotFoundTest extends GridCommonAbstractTest {
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
    public void testTemplateNotFound() throws Exception {
        Ignite ignite = startGrid(0);

        checkTemplateNotFound(ignite, "org.apache.ignite");

        checkTemplateNotFound(ignite, "org.apache.ignite.templat");

        checkTemplateNotFound(ignite, null);

        checkGetOrCreate(ignite, "org.apache.ignite.template", 3);

        CacheConfiguration templateCfg = new CacheConfiguration();

        templateCfg.setBackups(4);

        ignite.addCacheConfiguration(templateCfg);

        checkGetOrCreate(ignite, "org.apache.ignite", 4);

        checkGetOrCreate(ignite, null, 4);
    }

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    private void checkTemplateNotFound(final Ignite ignite, final String cacheName) {
        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                ignite.getOrCreateCache(cacheName);

                return null;
            }
        }, IllegalArgumentException.class, null);
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
