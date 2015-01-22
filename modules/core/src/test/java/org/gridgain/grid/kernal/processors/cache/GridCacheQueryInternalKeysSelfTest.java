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

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.cluster.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.datastructures.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.apache.ignite.internal.util.typedef.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.cache.GridCacheMode.*;
import static org.apache.ignite.cache.GridCachePreloadMode.*;

/**
 * Test for http://gridgain.jira.com/browse/GG-3979.
 */
public class GridCacheQueryInternalKeysSelfTest extends GridCacheAbstractSelfTest {
    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** Entry count. */
    private static final int ENTRY_CNT = 10;

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected GridCacheMode cacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cc = super.cacheConfiguration(gridName);

        cc.setQueryIndexEnabled(false);
        cc.setPreloadMode(SYNC);

        return cc;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testInternalKeysPreloading() throws Exception {
        try {
            GridCache<Object, Object> cache = grid(0).cache(null);

            for (int i = 0; i < ENTRY_CNT; i++)
                cache.dataStructures().queue("queue" + i, Integer.MAX_VALUE, false, true);

            startGrid(GRID_CNT); // Start additional node.

            for (int i = 0; i < ENTRY_CNT; i++) {
                GridCacheQueueHeaderKey internalKey = new GridCacheQueueHeaderKey("queue" + i);

                Collection<ClusterNode> nodes = cache.affinity().mapKeyToPrimaryAndBackups(internalKey);

                for (ClusterNode n : nodes) {
                    Ignite g = findGridForNodeId(n.id());

                    assertNotNull(g);

                    assertTrue("Affinity node doesn't contain internal key [key=" + internalKey + ", node=" + n + ']',
                        ((GridNearCacheAdapter)((GridKernal)g).internalCache()).dht().containsKey(internalKey, null));
                }
            }
        }
        finally {
            stopGrid(GRID_CNT);
        }
    }

    /**
     * Finds the {@link org.apache.ignite.Ignite}, which has a local node
     * with given ID.
     *
     * @param nodeId ID for grid's local node.
     * @return A grid instance or {@code null}, if the grid
     * is not found.
     */
    @Nullable private Ignite findGridForNodeId(final UUID nodeId) {
        return F.find(G.allGrids(), null, new P1<Ignite>() {
            @Override public boolean apply(Ignite e) {
                return nodeId.equals(e.cluster().localNode().id());
            }
        });
    }
}
