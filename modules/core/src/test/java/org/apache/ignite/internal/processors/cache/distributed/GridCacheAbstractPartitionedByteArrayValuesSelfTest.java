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

package org.apache.ignite.internal.processors.cache.distributed;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_TIERED;
import static org.apache.ignite.cache.CacheMemoryMode.OFFHEAP_VALUES;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for byte array values in PARTITIONED caches.
 */
public abstract class GridCacheAbstractPartitionedByteArrayValuesSelfTest extends
    GridCacheAbstractDistributedByteArrayValuesSelfTest {
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TransactionConfiguration tCfg = new TransactionConfiguration();

        tCfg.setTxSerializableEnabled(true);

        cfg.setTransactionConfiguration(tCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration0() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setNearConfiguration(nearConfiguration());
        cfg.setBackups(1);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setSwapEnabled(true);
        cfg.setEvictSynchronized(false);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration offheapCacheConfiguration0() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setNearConfiguration(nearConfiguration());
        cfg.setBackups(1);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setMemoryMode(OFFHEAP_VALUES);
        cfg.setOffHeapMaxMemory(100 * 1024 * 1024);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration offheapTieredCacheConfiguration0() {
        CacheConfiguration cfg = new CacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setAtomicityMode(TRANSACTIONAL);
        cfg.setNearConfiguration(nearConfiguration());
        cfg.setBackups(1);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setMemoryMode(OFFHEAP_TIERED);
        cfg.setOffHeapMaxMemory(100 * 1024 * 1024);

        return cfg;
    }

    /**
     * @return Distribution mode.
     */
    protected abstract NearCacheConfiguration nearConfiguration();
}