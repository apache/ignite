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

package org.apache.ignite.internal.client.thin;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** */
public class InactiveClusterCacheRequestTest extends AbstractThinClientTest {
    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        cfg.setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME));

        return cfg;
    }

    /** */
    @ParameterizedTest(name = "partitionAwareness={0}")
    @ValueSource(booleans = {true, false})
    public void testCacheOperationReturnErrorOnInactiveCluster(boolean partitionAwarenessEnabled) throws Exception {
        startGrids(2);

        ClientConfiguration ccfg = getClientConfiguration(grid(0), grid(1))
            .setPartitionAwarenessEnabled(partitionAwarenessEnabled);

        stopAllGrids();

        IgniteEx ign = startGrid(0);

        assertTrue(ign.cluster().state() == ClusterState.INACTIVE);

        try (IgniteClient cln = Ignition.startClient(ccfg)) {
            GridTestUtils.assertThrows(log,
                () -> cln.cache(DEFAULT_CACHE_NAME).get(0),
                ClientException.class,
                "cluster is inactive");
        }
    }
}
