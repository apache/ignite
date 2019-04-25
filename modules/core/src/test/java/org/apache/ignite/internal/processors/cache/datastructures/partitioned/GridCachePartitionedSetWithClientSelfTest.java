/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.datastructures.partitioned;

import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Tests IgniteSet with client node on {@code PARTITIONED} cache.
 */
public class GridCachePartitionedSetWithClientSelfTest extends GridCachePartitionedSetSelfTest {
    /** */
    private static final int CLIENT_NODE_IDX = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(getTestIgniteInstanceIndex(igniteInstanceName) == CLIENT_NODE_IDX);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void testCreateRemove(boolean collocated) throws Exception {
        testCreateRemove(collocated, CLIENT_NODE_IDX);
    }

    /** {@inheritDoc} */
    @Override protected void testIterator(boolean collocated) throws Exception {
        testIterator(collocated, CLIENT_NODE_IDX);
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 5;
    }
}
