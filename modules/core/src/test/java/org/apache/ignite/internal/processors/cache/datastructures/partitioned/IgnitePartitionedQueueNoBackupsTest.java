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

package org.apache.ignite.internal.processors.cache.datastructures.partitioned;

import java.util.Iterator;
import java.util.UUID;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheMapEntry;
import org.apache.ignite.testframework.GridTestUtils;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;

/**
 *
 */
public class IgnitePartitionedQueueNoBackupsTest extends GridCachePartitionedQueueApiSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheMode collectionCacheMode() {
        return PARTITIONED;
    }

    /** {@inheritDoc} */
    @Override protected CacheAtomicityMode collectionCacheAtomicityMode() {
        return ATOMIC;
    }

    /** {@inheritDoc} */
    @Override protected CollectionConfiguration collectionConfiguration() {
        CollectionConfiguration colCfg = super.collectionConfiguration();

        colCfg.setBackups(0);

        return colCfg;
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCollocation() throws Exception {
        IgniteQueue<Integer> queue = grid(0).queue("queue", 0, config(true));

        for (int i = 0; i < 1000; i++)
            assertTrue(queue.add(i));

        assertEquals(1000, queue.size());

        GridCacheContext cctx = GridTestUtils.getFieldValue(queue, "cctx");

        UUID setNodeId = null;

        for (int i = 0; i < gridCount(); i++) {
            IgniteKernal grid = (IgniteKernal)grid(i);

            GridCacheAdapter cache = grid.context().cache().internalCache(cctx.name());

            Iterator<GridCacheMapEntry> entries = cache.map().entries(cache.context().cacheId()).iterator();

            if (entries.hasNext()) {
                if (setNodeId == null)
                    setNodeId = grid.localNode().id();
                else
                    fail("For collocated queue all items should be stored on single node.");
            }
        }
    }
}
