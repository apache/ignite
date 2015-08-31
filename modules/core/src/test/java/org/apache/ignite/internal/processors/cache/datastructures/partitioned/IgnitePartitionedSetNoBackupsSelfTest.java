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
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheEntryEx;
import org.apache.ignite.testframework.GridTestUtils;

/**
 *
 */
public class IgnitePartitionedSetNoBackupsSelfTest extends GridCachePartitionedSetSelfTest {
    /** {@inheritDoc} */
    @Override protected CollectionConfiguration collectionConfiguration() {
        CollectionConfiguration colCfg = super.collectionConfiguration();

        colCfg.setBackups(0);

        return colCfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testCollocation() throws Exception {
        Set<Integer> set0 = grid(0).set(SET_NAME, config(true));

        for (int i = 0; i < 1000; i++)
            assertTrue(set0.add(i));

        assertEquals(1000, set0.size());

        GridCacheContext cctx = GridTestUtils.getFieldValue(set0, "cctx");

        UUID setNodeId = null;

        for (int i = 0; i < gridCount(); i++) {
            IgniteKernal grid = (IgniteKernal)grid(i);

            Iterator<GridCacheEntryEx> entries =
                grid.context().cache().internalCache(cctx.name()).map().allEntries0().iterator();

            if (entries.hasNext()) {
                if (setNodeId == null)
                    setNodeId = grid.localNode().id();
                else
                    fail("For collocated set all items should be stored on single node.");
            }
        }
    }
}