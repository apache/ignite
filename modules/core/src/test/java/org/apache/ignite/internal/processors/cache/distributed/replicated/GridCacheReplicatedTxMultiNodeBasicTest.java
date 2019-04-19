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

package org.apache.ignite.internal.processors.cache.distributed.replicated;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.processors.cache.IgniteTxMultiNodeAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Test basic cache operations in transactions.
 */
public class GridCacheReplicatedTxMultiNodeBasicTest extends IgniteTxMultiNodeAbstractTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        // Default cache configuration.
        CacheConfiguration ccfg = defaultCacheConfiguration();

        ccfg.setCacheMode(REPLICATED);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testPutOneEntryInTx() throws Exception {
        super.testPutOneEntryInTx();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testPutTwoEntriesInTx() throws Exception {
        super.testPutTwoEntriesInTx();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testPutOneEntryInTxMultiThreaded() throws Exception {
        super.testPutOneEntryInTxMultiThreaded();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testPutTwoEntryInTxMultiThreaded() throws Exception {
        super.testPutTwoEntryInTxMultiThreaded();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRemoveInTxQueried() throws Exception {
        super.testRemoveInTxQueried();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRemoveInTxSimple() throws Exception {
        super.testRemoveInTxSimple();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRemoveInTxQueriedMultiThreaded() throws Exception {
        super.testRemoveInTxQueriedMultiThreaded();
    }
}
