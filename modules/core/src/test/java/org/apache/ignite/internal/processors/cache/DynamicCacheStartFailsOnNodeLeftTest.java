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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Test the dynamic cache startup failure feature if the required node leaves the topology during cache startup.
 */
public class DynamicCacheStartFailsOnNodeLeftTest extends GridCommonAbstractTest {
    /** Test cache name, */
    private static final String TEST_CACHE = "testCache";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setCommunicationSpi(new TestRecordingCommunicationSpi());
    }

    /** {@inheritDoc} */
    @Override public void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollbackOnRequiredTopologyWhenCrdFailsOnExhangeFinish() throws Exception {
        checkRollbackOnRequiredTopologyWhenNodeFails(true);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testRollbackOnRequiredTopologyWhenNodeFailsOnExhangeFinish() throws Exception {
        checkRollbackOnRequiredTopologyWhenNodeFails(false);
    }


    /**
     * @param crdStop {{@code True} to stop coordinator node.
     */
    public void checkRollbackOnRequiredTopologyWhenNodeFails(boolean crdStop) throws Exception {
        startGridsMultiThreaded(3);

        Collection<UUID> srvNodes = F.viewReadOnly(grid(0).cluster().nodes(), F.node2id());

        IgniteEx client = startClientGrid();

        TestRecordingCommunicationSpi node1spi = TestRecordingCommunicationSpi.spi(grid(1));
        TestRecordingCommunicationSpi node2spi = TestRecordingCommunicationSpi.spi(grid(2));

        node1spi.blockMessages((node, msg) -> msg instanceof GridDhtPartitionsSingleMessage);
        node2spi.blockMessages((node, msg) -> msg instanceof GridDhtPartitionsSingleMessage);

        StoredCacheData storedConf = new StoredCacheData(new CacheConfiguration<>(TEST_CACHE));
        
        IgniteInternalFuture<Boolean> fut = client.context().cache().dynamicStartCachesByStoredConf(
            Collections.singleton(storedConf), true, true, false, null, true, new HashSet<>(srvNodes));

        node1spi.waitForBlocked();
        node2spi.waitForBlocked();

        stopGrid(crdStop ? 0 : 2, true);

        node1spi.stopBlock();

        if (crdStop)
            node2spi.stopBlock();

        boolean exFound = false;

        try {
            fut.get(getTestTimeout());
        } catch (Exception e) {
            for (Throwable t : e.getSuppressed()) {
                if (t.getClass().equals(ClusterTopologyCheckedException.class) &&
                    t.getMessage().contains("Required node has left the cluster")) {
                    exFound = true;

                    break;
                }
            }
        }

        assertTrue(exFound);

        awaitPartitionMapExchange();

        for (Ignite grid : G.allGrids())
            assertNull(((IgniteEx)grid).context().cache().cacheGroup(CU.cacheId(TEST_CACHE)));

        // Make sure the cache can be successfully created.
        client.createCache(storedConf.config());
    }
}
