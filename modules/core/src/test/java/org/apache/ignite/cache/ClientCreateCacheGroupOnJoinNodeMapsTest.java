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

package org.apache.ignite.cache;

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionFullMap;
import org.apache.ignite.internal.processors.cache.distributed.dht.topology.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests if node maps are correct for caches created on client node join.
 */
public class ClientCreateCacheGroupOnJoinNodeMapsTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        final IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setActiveOnStart(false);

        boolean client = "client".equals(igniteInstanceName);

        cfg.setClientMode(client);

        if (client) {
            cfg.setCacheConfiguration(defaultCacheConfiguration().setNearConfiguration(null).
                setAffinity(new RendezvousAffinityFunction(false, 32)));
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

    /** */
    @Test
    public void testNodeMapsVolatile() throws Exception {
        final IgniteEx srv = startGrids(2);

        srv.cluster().active(true);

        awaitPartitionMapExchange();

        final IgniteEx client = startGrid("client");

        final GridDhtPartitionTopology top0 = grid(0).cachex(DEFAULT_CACHE_NAME).context().topology();
        final GridDhtPartitionTopology top1 = grid(1).cachex(DEFAULT_CACHE_NAME).context().topology();

        GridDhtPartitionFullMap map0 = U.field(top0, "node2part");
        GridDhtPartitionFullMap map1 = U.field(top1, "node2part");

        assertEquals(2, map0.size());
        assertEquals(2, map1.size());

        srv.cluster().active(false);
    }
}
