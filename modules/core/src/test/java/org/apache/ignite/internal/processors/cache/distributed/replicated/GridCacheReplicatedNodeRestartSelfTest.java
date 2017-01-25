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

package org.apache.ignite.internal.processors.cache.distributed.replicated;

import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.GridCacheAbstractNodeRestartSelfTest;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests node restart.
 */
public class GridCacheReplicatedNodeRestartSelfTest extends GridCacheAbstractNodeRestartSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setNearConfiguration(null);

        cc.setAtomicityMode(atomicityMode());

        cc.setName(CACHE_NAME);

        cc.setCacheMode(REPLICATED);

        cc.setWriteSynchronizationMode(FULL_SYNC);

        cc.setStartSize(20);

        cc.setRebalanceMode(SYNC);

        cc.setRebalanceBatchSize(20);

        return cc;
    }

    /** {@inheritDoc} */
    @Override public void testRestartWithPutTwoNodesNoBackups() throws Throwable {
        super.testRestartWithPutTwoNodesNoBackups();
    }

    /** {@inheritDoc} */
    @Override public void testRestartWithPutTwoNodesOneBackup() throws Throwable {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void testRestartWithPutFourNodesOneBackups() throws Throwable {
        super.testRestartWithPutFourNodesOneBackups();
    }

    /** {@inheritDoc} */
    @Override public void testRestartWithPutFourNodesNoBackups() throws Throwable {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void testRestartWithPutSixNodesTwoBackups() throws Throwable {
        super.testRestartWithPutSixNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Override public void testRestartWithPutEightNodesTwoBackups() throws Throwable {
        super.testRestartWithPutEightNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Override public void testRestartWithPutTenNodesTwoBackups() throws Throwable {
        super.testRestartWithPutTenNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Override public void testRestartWithTxTwoNodesNoBackups() throws Throwable {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void testRestartWithTxTwoNodesOneBackup() throws Throwable {
        super.testRestartWithTxTwoNodesOneBackup();
    }

    /** {@inheritDoc} */
    @Override public void testRestartWithTxFourNodesOneBackups() throws Throwable {
        super.testRestartWithTxFourNodesOneBackups();
    }

    /** {@inheritDoc} */
    @Override public void testRestartWithTxFourNodesNoBackups() throws Throwable {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void testRestartWithTxSixNodesTwoBackups() throws Throwable {
        super.testRestartWithTxSixNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Override public void testRestartWithTxEightNodesTwoBackups() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Override public void testRestartWithTxTenNodesTwoBackups() throws Throwable {
        super.testRestartWithTxTenNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Override public void testRestartWithTxPutAllTenNodesTwoBackups() throws Throwable {
        super.testRestartWithTxPutAllTenNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Override public void testRestartWithTxPutAllFourNodesTwoBackups() throws Throwable {
        super.testRestartWithTxPutAllFourNodesTwoBackups();
    }
}
