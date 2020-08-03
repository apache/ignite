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

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.GridCacheProcessor;
import org.apache.ignite.internal.processors.cache.IgniteTxMultiThreadedAbstractTest;
import org.apache.log4j.Level;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for partitioned cache transactions.
 */
public class GridCachePartitionedTxMultiThreadedSelfTest extends IgniteTxMultiThreadedAbstractTest {
    /** Cache debug flag. */
    private static final boolean CACHE_DEBUG = false;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.getTransactionConfiguration().setTxSerializableEnabled(true);

        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setBackups(1);

        cc.setEvictionPolicy(null);

        cc.setWriteSynchronizationMode(FULL_SYNC);

        cc.setNearConfiguration(nearEnabled() ? new NearCacheConfiguration() : null);

        c.setCacheConfiguration(cc);

        if (CACHE_DEBUG)
            resetLog4j(Level.DEBUG, true, GridCacheProcessor.class.getPackage().getName());

        return c;
    }

    /**
     * @return {@code True} if near cache is enabled.
     */
    protected boolean nearEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected int gridCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int keyCount() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int maxKeyValue() {
        return 3;
    }

    /** {@inheritDoc} */
    @Override protected int threadCount() {
        return 5;
    }

    /** {@inheritDoc} */
    @Override protected int iterations() {
        return 1000;
    }

    /** {@inheritDoc} */
    @Override protected boolean isTestDebug() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected boolean printMemoryStats() {
        return true;
    }
}
