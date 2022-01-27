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

package org.apache.ignite.cache.affinity.rendezvous;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionExcludeNeighborsAbstractSelfTest;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.junit.Test;

/**
 * Tests exclude neighbors flag for rendezvous affinity function.
 */
public class RendezvousAffinityFunctionExcludeNeighborsSelfTest extends
    AffinityFunctionExcludeNeighborsAbstractSelfTest {
    /** With backup filter flag. */
    private boolean withBackupFilter;

    /** {@inheritDoc} */
    @Override protected AffinityFunction affinityFunction() {
        if (withBackupFilter) {
            return new RendezvousAffinityFunction(true)
                    .setAffinityBackupFilter((c, l) -> backupFilter(l.get(0)) == backupFilter(c));
        }
        else
            return new RendezvousAffinityFunction(true);
    }

    /** Checks that exclude neighbors flag and backup filter works together. */
    @Test
    public void testAffinityWithBackupFilter() throws Exception {
        int grids = 9;
        withBackupFilter = true;
        int copies = backups + 1;

        try {
            startGrids(grids);

            awaitPartitionMapExchange();

            Affinity<Integer> aff = grid(0).affinity(DEFAULT_CACHE_NAME);

            for (int i = 0; i < aff.partitions(); i++) {
                Collection<ClusterNode> affNodes = aff.mapKeyToPrimaryAndBackups(i);

                assertEquals(copies, affNodes.size());

                Set<String> macs = new HashSet<>();

                long backupFilterKey = -1L;

                for (ClusterNode node : affNodes) {
                    macs.add(node.attribute(IgniteNodeAttributes.ATTR_MACS));

                    if (backupFilterKey < 0)
                        backupFilterKey = backupFilter(node);
                    else
                        assertEquals(backupFilterKey, backupFilter(node));
                }

                assertEquals(copies, macs.size());
            }
        }
        finally {
            withBackupFilter = false;

            stopAllGrids();
        }
    }

    /** Value for filtering nodes by backup filter. */
    private static long backupFilter(ClusterNode node) {
        return node.order() % 3;
    }
}
