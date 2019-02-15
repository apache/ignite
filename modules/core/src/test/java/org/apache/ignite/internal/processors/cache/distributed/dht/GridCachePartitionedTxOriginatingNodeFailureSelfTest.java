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

package org.apache.ignite.internal.processors.cache.distributed.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.IgniteTxOriginatingNodeFailureAbstractSelfTest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Tests transaction consistency when originating node fails.
 */
@RunWith(JUnit4.class)
public class GridCachePartitionedTxOriginatingNodeFailureSelfTest extends
    IgniteTxOriginatingNodeFailureAbstractSelfTest {
    /** */
    private static final int BACKUP_CNT = 2;

    /** {@inheritDoc} */
    @Override protected CacheMode cacheMode() {
        return CacheMode.PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String igniteInstanceName) throws Exception {
        CacheConfiguration ccfg = super.cacheConfiguration(igniteInstanceName);

        ccfg.setBackups(BACKUP_CNT);

        return ccfg;
    }

    /** {@inheritDoc} */
    @Override protected Class<?> ignoreMessageClass() {
        return GridNearTxPrepareRequest.class;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxFromPrimary() throws Exception {
        ClusterNode txNode = grid(originatingNode()).localNode();

        Integer key = null;

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (grid(originatingNode()).affinity(DEFAULT_CACHE_NAME).isPrimary(txNode, i)) {
                key = i;

                break;
            }
        }

        assertNotNull(key);

        testTxOriginatingNodeFails(Collections.singleton(key), false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxFromBackup() throws Exception {
        ClusterNode txNode = grid(originatingNode()).localNode();

        Integer key = null;

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (grid(originatingNode()).affinity(DEFAULT_CACHE_NAME).isBackup(txNode, i)) {
                key = i;

                break;
            }
        }

        assertNotNull(key);

        testTxOriginatingNodeFails(Collections.singleton(key), false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxFromNotColocated() throws Exception {
        ClusterNode txNode = grid(originatingNode()).localNode();

        Integer key = null;

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            if (!grid(originatingNode()).affinity(DEFAULT_CACHE_NAME).isPrimary(txNode, i)
                && !grid(originatingNode()).affinity(DEFAULT_CACHE_NAME).isBackup(txNode, i)) {
                key = i;

                break;
            }
        }

        assertNotNull(key);

        testTxOriginatingNodeFails(Collections.singleton(key), false);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTxAllNodes() throws Exception {
        List<ClusterNode> allNodes = new ArrayList<>(GRID_CNT);

        for (int i = 0; i < GRID_CNT; i++)
            allNodes.add(grid(i).localNode());

        Collection<Integer> keys = new ArrayList<>();

        for (int i = 0; i < Integer.MAX_VALUE && !allNodes.isEmpty(); i++) {
            for (Iterator<ClusterNode> iter = allNodes.iterator(); iter.hasNext();) {
                ClusterNode node = iter.next();

                if (grid(originatingNode()).affinity(DEFAULT_CACHE_NAME).isPrimary(node, i)) {
                    keys.add(i);

                    iter.remove();

                    break;
                }
            }
        }

        assertEquals(GRID_CNT, keys.size());

        testTxOriginatingNodeFails(keys, false);
    }
}
