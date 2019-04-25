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

import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.util.AttributeNodeFilter;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;

/**
 * Tests IgniteSet with node filter on {@code PARTITIONED} cache.
 */
public class GridCachePartitionedSetWithNodeFilterSelfTest extends GridCachePartitionedSetSelfTest {
    /** {@inheritDoc} */
    @Override protected CollectionConfiguration collectionConfiguration() {
        CollectionConfiguration cfg = super.collectionConfiguration();

        cfg.setNodeFilter(new AttributeNodeFilter(ATTR_IGNITE_INSTANCE_NAME, getTestIgniteInstanceName(0)));

        return cfg;
    }
}
