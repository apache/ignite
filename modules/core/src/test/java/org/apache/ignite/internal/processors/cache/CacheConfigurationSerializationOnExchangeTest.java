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

import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test suite to check that user-defined parameters (marked as {@link org.apache.ignite.configuration.SerializeSeparately})
 * for dynamic cache configurations are not explicitly deserialized on non-affinity nodes.
 */
@RunWith(Parameterized.class)
public class CacheConfigurationSerializationOnExchangeTest extends CacheConfigurationSerializationAbstractTest {
    /**
     *
     */
    @Test
    public void testSerializationForDynamicCacheStartedOnCoordinator() throws Exception {
        IgniteEx crd = (IgniteEx)startGridsMultiThreaded(3);

        if (persistenceEnabled)
            crd.cluster().state(ClusterState.ACTIVE);

        startClientGrid(3);

        crd.getOrCreateCaches(Arrays.asList(onlyOnNode(0), onlyOnNode(1), onlyOnNode(2)));

        awaitPartitionMapExchange();

        for (Ignite node : G.allGrids())
            checkCaches((IgniteEx) node);

        restartNodesAndCheck(persistenceEnabled);
    }

    /**
     *
     */
    @Test
    public void testSerializationForDynamicCacheStartedOnOtherNode() throws Exception {
        startGridsMultiThreaded(2);

        IgniteEx otherNode = startGrid(2);

        if (persistenceEnabled)
            otherNode.cluster().state(ClusterState.ACTIVE);

        startClientGrid(3);

        otherNode.getOrCreateCaches(Arrays.asList(onlyOnNode(0), onlyOnNode(1), onlyOnNode(2)));

        awaitPartitionMapExchange();

        for (Ignite node : G.allGrids())
            checkCaches((IgniteEx) node);

        restartNodesAndCheck(persistenceEnabled);
    }

    /**
     *
     */
    @Test
    public void testSerializationForDynamicCacheStartedOnClientNode() throws Exception {
        IgniteEx crd = (IgniteEx)startGridsMultiThreaded(3);

        if (persistenceEnabled)
            crd.cluster().state(ClusterState.ACTIVE);

        IgniteEx clientNode = startClientGrid(3);

        clientNode.getOrCreateCaches(Arrays.asList(onlyOnNode(0), onlyOnNode(1), onlyOnNode(2)));

        awaitPartitionMapExchange();

        for (Ignite node : G.allGrids())
            checkCaches((IgniteEx)node);

        restartNodesAndCheck(persistenceEnabled);
    }
}
