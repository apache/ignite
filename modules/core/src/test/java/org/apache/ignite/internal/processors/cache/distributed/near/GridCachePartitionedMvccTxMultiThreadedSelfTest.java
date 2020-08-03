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
import org.apache.ignite.internal.processors.cache.IgniteMvccTxMultiThreadedAbstractTest;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.junit.Assume;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests for partitioned cache transactions.
 */
public class GridCachePartitionedMvccTxMultiThreadedSelfTest extends IgniteMvccTxMultiThreadedAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        if (nearEnabled())
            Assume.assumeTrue(MvccFeatureChecker.isSupported(MvccFeatureChecker.Feature.NEAR_CACHE));

        super.beforeTestsStarted();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration<?, ?> ccfg = defaultCacheConfiguration();

        ccfg.setAtomicityMode(TRANSACTIONAL_SNAPSHOT);
        ccfg.setCacheMode(PARTITIONED);
        ccfg.setBackups(1);

        ccfg.setEvictionPolicy(null);

        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        ccfg.setNearConfiguration(nearEnabled() ? new NearCacheConfiguration() : null);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
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
