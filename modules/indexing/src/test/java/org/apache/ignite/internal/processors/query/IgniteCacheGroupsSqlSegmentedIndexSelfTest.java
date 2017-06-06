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

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.configuration.CacheConfiguration;

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
    @Override public void testSegmentedPartitionedWithReplicated() throws Exception {
        log.info("Test is ignored");
    }
}
