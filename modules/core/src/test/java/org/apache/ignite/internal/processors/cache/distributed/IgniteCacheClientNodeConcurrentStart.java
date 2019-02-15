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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;

/**
 *
 */
public class IgniteCacheClientNodeConcurrentStart extends GridCommonAbstractTest {
    /** */
    private static final int NODES_CNT = 6;

    /** */
    private Set<Integer> clientNodes;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        assertNotNull(clientNodes);

        boolean client = false;

        for (Integer clientIdx : clientNodes) {
            if (getTestIgniteInstanceName(clientIdx).equals(igniteInstanceName)) {
                client = true;

                break;
            }
        }

        cfg.setClientMode(client);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setBackups(0);
        ccfg.setRebalanceMode(SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentStart() throws Exception {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 3; i++) {
            try {
                clientNodes = new HashSet<>();

                while (clientNodes.size() < 2)
                    clientNodes.add(rnd.nextInt(1, NODES_CNT));

                clientNodes.add(NODES_CNT - 1);

                log.info("Test iteration [iter=" + i + ", clients=" + clientNodes + ']');

                Ignite srv = startGrid(0); // Start server node first.

                assertFalse(srv.configuration().isClientMode());

                startGridsMultiThreaded(1, NODES_CNT - 1);

                checkTopology(NODES_CNT);

                awaitPartitionMapExchange();

                for (int node : clientNodes) {
                    Ignite ignite = grid(node);

                    assertTrue(ignite.configuration().isClientMode());
                }
            }
            finally {
                stopAllGrids();
            }
        }
    }
}
