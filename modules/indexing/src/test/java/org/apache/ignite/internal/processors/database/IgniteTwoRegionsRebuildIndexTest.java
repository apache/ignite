/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests the case when preformed index rebuild for created by client in-memory cache.
 */
@RunWith(JUnit4.class)
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
            cfg.setClientMode(true);
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
        Ignite client = startGrid("client");

        client.cluster().active(true);

        populateData(client, PERSISTED_CACHE);
        populateData(client, INMEMORY_CACHE);

        stopGrid("server");
        startGrid("server");

        stopGrid("client");
        startGrid("client");
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
