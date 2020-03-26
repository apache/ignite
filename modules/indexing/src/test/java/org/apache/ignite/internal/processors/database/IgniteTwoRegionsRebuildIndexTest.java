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

package org.apache.ignite.internal.processors.database;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests the case when preformed index rebuild for created by client in-memory cache.
 */
public class IgniteTwoRegionsRebuildIndexTest extends GridCommonAbstractTest {
    /** */
    private static final String PERSISTED_CACHE = "persisted";

    /** */
    private static final String INMEMORY_CACHE = "inmemory";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        boolean client = igniteInstanceName.startsWith("client");

        DataStorageConfiguration dsCfg = new DataStorageConfiguration();

        if (!client) {
            DataRegionConfiguration drCfg1 = new DataRegionConfiguration();
            drCfg1.setMaxSize(16 * 1024 * 1024);
            drCfg1.setName("nopersistence");
            drCfg1.setInitialSize(drCfg1.getMaxSize());
            drCfg1.setPersistenceEnabled(false);

            DataRegionConfiguration drCfg2 = new DataRegionConfiguration();
            drCfg2.setMaxSize(16 * 1024 * 1024);
            drCfg2.setName("persistence");
            drCfg2.setInitialSize(drCfg2.getMaxSize());
            drCfg2.setPersistenceEnabled(true);

            dsCfg.setDataRegionConfigurations(drCfg1, drCfg2);

            cfg.setDataStorageConfiguration(dsCfg);
        }
        else {
            CacheConfiguration ccfg1 = new CacheConfiguration(PERSISTED_CACHE);
            CacheConfiguration ccfg2 = new CacheConfiguration(INMEMORY_CACHE);

            ccfg1.setDataRegionName("persistence");
            ccfg2.setDataRegionName("nopersistence");

            cfg.setCacheConfiguration(ccfg1, ccfg2);
        }

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRebuildIndexes() throws Exception {
        startGrid("server");
        Ignite client = startClientGrid("client");

        client.cluster().active(true);

        populateData(client, PERSISTED_CACHE);
        populateData(client, INMEMORY_CACHE);

        stopGrid("server");
        startGrid("server");

        stopGrid("client");
        startClientGrid("client");
    }

    /**
     * @param ignite Ignite.
     * @param cacheName Cache name.
     */
    private void populateData(Ignite ignite, String cacheName) {
        try (IgniteDataStreamer<Object, Object> streamer = ignite.dataStreamer(cacheName)) {
            for (int i = 0; i < 1000; i++)
                streamer.addData(i, i);

            streamer.flush();
        }
    }
}
