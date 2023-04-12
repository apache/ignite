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

import java.util.List;
import org.apache.ignite.Ignite;
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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** */
@RunWith(Parameterized.class)
public class InactiveClusterCacheRequestTest extends AbstractThinClientTest {
    /** */
    @Parameterized.Parameter
    public boolean partitionAwarenessEnabled;

    /** */
    @Parameterized.Parameters(name = "partitionAwareness={0}")
    public static List<Boolean> params() {
        return F.asList(false, true);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setDataStorageConfiguration(new DataStorageConfiguration()
            .setDefaultDataRegionConfiguration(new DataRegionConfiguration().setPersistenceEnabled(true)));

        cfg.setCacheConfiguration(new CacheConfiguration("default"));

        return cfg;
    }

    /** */
    @Test
    public void testCacheOperationReturnErrorOnInactiveCluster() throws Exception {
        IgniteEx ign = startGrids(2);

        ign.cluster().state(ClusterState.ACTIVE);

        ClientConfiguration ccfg = getClientConfiguration(G.allGrids().toArray(new Ignite[]{}))
            .setPartitionAwarenessEnabled(partitionAwarenessEnabled);

        stopAllGrids();

        startGrid(0);

        IgniteClient cln = Ignition.startClient(ccfg);

        GridTestUtils.assertThrows(log,
            () -> cln.cache("default").get(0),
            ClientException.class,
            "cluster is inactive");
    }
}
