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

package org.apache.ignite.internal.processors.cache.distributed.near;

import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.processors.cache.*;

import static org.apache.ignite.cache.CacheAtomicWriteOrderMode.*;
import static org.apache.ignite.cache.CacheAtomicityMode.*;
import static org.apache.ignite.cache.CacheMode.*;
import static org.apache.ignite.cache.CachePreloadMode.*;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.*;

/**
 * Partitioned atomic cache metrics test.
 */
public class GridCacheAtomicPartitionedMetricsSelfTest extends GridCacheAbstractMetricsSelfTest {
    /** */
    private static final int GRID_CNT = 2;

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration(String gridName) throws Exception {
        CacheConfiguration cfg = super.cacheConfiguration(gridName);

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(gridCount() - 1);
        cfg.setPreloadMode(SYNC);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setAtomicityMode(ATOMIC);
        cfg.setAtomicWriteOrderMode(PRIMARY);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return GRID_CNT;
    }

    /** {@inheritDoc} */
    @Override protected int expectedReadsPerPut(boolean isPrimary) {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected int expectedMissesPerPut(boolean isPrimary) {
        return 1;
    }
}
