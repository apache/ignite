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

import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheAbstractNodeRestartSelfTest;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;

/**
 * Test node restart.
 */
public class GridCachePartitionedOptimisticTxNodeRestartTest extends GridCacheAbstractNodeRestartSelfTest {
    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(igniteInstanceName);

        c.getTransactionConfiguration().setDefaultTxConcurrency(OPTIMISTIC);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setName(CACHE_NAME);
        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setRebalanceMode(rebalancMode);
        cc.setRebalanceBatchSize(rebalancBatchSize);
        cc.setAffinity(new RendezvousAffinityFunction(false, partitions));
        cc.setBackups(backups);

        cc.setNearConfiguration(nearEnabled() ? new NearCacheConfiguration() : null);

        return cc;
    }

    /**
     * @return {@code True} if near cache enabled.
     */
    protected boolean nearEnabled() {
        return true;
    }

    /** {@inheritDoc} */
    @Override protected TransactionConcurrency txConcurrency() {
        return OPTIMISTIC;
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestart() throws Exception {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutTwoNodesNoBackups() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutTwoNodesOneBackup() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutFourNodesNoBackups() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutFourNodesOneBackups() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutSixNodesTwoBackups() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutEightNodesTwoBackups() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithPutTenNodesTwoBackups() throws Throwable {
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxEightNodesTwoBackups() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxFourNodesNoBackups() throws Throwable {
        super.testRestartWithTxFourNodesNoBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxFourNodesOneBackups() throws Throwable {
        super.testRestartWithTxFourNodesOneBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxSixNodesTwoBackups() throws Throwable {
        super.testRestartWithTxSixNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxTenNodesTwoBackups() throws Throwable {
        super.testRestartWithTxTenNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxTwoNodesNoBackups() throws Throwable {
        super.testRestartWithTxTwoNodesNoBackups();
    }

    /** {@inheritDoc} */
    @Test
    @Override public void testRestartWithTxTwoNodesOneBackup() throws Throwable {
        super.testRestartWithTxTwoNodesOneBackup();
    }
}
