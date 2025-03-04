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

package org.apache.ignite.internal.processors.cache.persistence;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public class IgnitePdsOnClientTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        if (IgnitionEx.isClientMode())
            cfg.getDataStorageConfiguration().setStoragePath("/unexisting/storage/path");

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        cleanPersistenceDir();
    }

    /**
     * Tests client node can start with {@link DataStorageConfiguration#getStoragePath()} configuration.
     * Client node doesn't store data so {@link DataStorageConfiguration#getStoragePath()} should be ignored.
     */
    @Test
    public void testStartClientWithPersistenceConfiguration() throws Exception {
        try (Ignite ignite = startGrid(0)) {
            IgniteEx client = startClientGrid(1);

            client.cluster().state(ClusterState.ACTIVE);

            IgniteCache<Integer, Integer> cache = client.createCache("my-cache");

            cache.put(1, 1);
            assertEquals((Integer)1, cache.get(1));

            // Checking there are no files created on client node start.
            assertFalse(client.context().pdsFolderResolver().fileTree().nodeStorage().exists());
        }
    }
}
