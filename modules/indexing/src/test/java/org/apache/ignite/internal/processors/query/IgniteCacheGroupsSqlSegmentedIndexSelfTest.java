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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Test;

/**
 *
 */
public class IgniteCacheGroupsSqlSegmentedIndexSelfTest extends IgniteSqlSegmentedIndexSelfTest {
    /** {@inheritDoc} */
    @Override protected <K, V> CacheConfiguration<K, V> cacheConfig(String name, boolean partitioned, Class<?>... idxTypes) {
        return super.<K, V>cacheConfig(name, partitioned, idxTypes).setGroupName("group");
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    @Override public void testSegmentedPartitionedWithReplicated() throws Exception {
        log.info("Test is ignored");
    }
}
