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

package org.apache.ignite.internal.processors.query.h2;

import org.apache.ignite.configuration.CacheConfiguration;

/**
 * Test for segmented geo index.
 */
public class GridH2IndexingSegmentedGeoSelfTest extends GridH2IndexingGeoSelfTest {
    /** */
    private static int QRY_PARALLELISM_LVL = 7;

    /** {@inheritDoc} */
    @Override
    protected <K, V> CacheConfiguration<K, V> cacheConfig(String name, boolean partitioned,
        Class<?>... idxTypes) throws Exception {
        final CacheConfiguration<K, V> ccfg = super.cacheConfig(name, partitioned, idxTypes);

        return ccfg.setQueryParallelism(partitioned ? QRY_PARALLELISM_LVL : 1);
    }
}