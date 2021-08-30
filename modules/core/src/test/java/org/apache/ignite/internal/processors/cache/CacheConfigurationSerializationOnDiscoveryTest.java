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
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Test suite to check that user-defined parameters (marked as {@link org.apache.ignite.configuration.SerializeSeparately})
 * for static cache configurations are not explicitly deserialized on non-affinity nodes.
 */
@RunWith(Parameterized.class)
public class CacheConfigurationSerializationOnDiscoveryTest extends CacheConfigurationSerializationAbstractTest {
    /** Caches. */
    private CacheConfiguration[] caches;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (caches != null)
            cfg.setCacheConfiguration(caches);

        return cfg;
    }

    /**
     *
     */
    @Test
    public void testSerializationForCachesConfiguredOnCoordinator() throws Exception {
        caches = new CacheConfiguration[] {onlyOnNode(0), onlyOnNode(1), onlyOnNode(2)};

        IgniteEx crd = startGrid(0);

        caches = null;

        startGridsMultiThreaded(1, 2);

        if (persistenceEnabled)
            crd.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        for (Ignite node : G.allGrids())
            checkCaches((IgniteEx) node);

        restartNodesAndCheck(persistenceEnabled);
    }

    /**
     *
     */
    @Test
    public void testSerializationForCachesConfiguredOnDifferentNodes1() throws Exception {
        IgniteEx crd = startGrid(0);

        caches = new CacheConfiguration[] {onlyOnNode(0), onlyOnNode(1)};

        startGrid(1);

        caches = new CacheConfiguration[] {onlyOnNode(2)};

        startGrid(2);

        caches = null;

        if (persistenceEnabled)
            crd.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        for (Ignite node : G.allGrids())
            checkCaches((IgniteEx) node);

        restartNodesAndCheck(persistenceEnabled);
    }

    /**
     *
     */
    @Test
    public void testSerializationForCachesConfiguredOnDifferentNodes2() throws Exception {
        caches = new CacheConfiguration[] {onlyOnNode(0)};

        IgniteEx crd = startGrid(0);

        caches = new CacheConfiguration[] {onlyOnNode(1)};

        startGrid(1);

        caches = new CacheConfiguration[] {onlyOnNode(2)};

        startGrid(2);

        caches = null;

        if (persistenceEnabled)
            crd.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        for (Ignite node : G.allGrids())
            checkCaches((IgniteEx) node);

        restartNodesAndCheck(persistenceEnabled);
    }

    /**
     *
     */
    @Test
    public void testSerializationForCachesConfiguredOnDifferentNodes3() throws Exception {
        caches = new CacheConfiguration[] {onlyOnNode(1)};

        IgniteEx crd = startGrid(0);

        caches = new CacheConfiguration[] {onlyOnNode(2)};

        startGrid(1);

        caches = new CacheConfiguration[] {onlyOnNode(0)};

        startGrid(2);

        caches = null;

        if (persistenceEnabled)
            crd.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        for (Ignite node : G.allGrids())
            checkCaches((IgniteEx) node);

        restartNodesAndCheck(persistenceEnabled);
    }

    /**
     *
     */
    @Test
    public void testSerializationForCachesOnClientNode() throws Exception {
        startGrid(0);

        caches = new CacheConfiguration[] {onlyOnNode(0), onlyOnNode(1)};

        startGrid(1);

        caches = new CacheConfiguration[] {onlyOnNode(2)};

        startGrid(2);

        caches = null;

        IgniteEx clnt = startClientGrid(3);

        if (persistenceEnabled)
            clnt.cluster().state(ClusterState.ACTIVE);

        awaitPartitionMapExchange();

        for (Ignite node : G.allGrids())
            checkCaches((IgniteEx) node);

        restartNodesAndCheck(persistenceEnabled);
    }
}
