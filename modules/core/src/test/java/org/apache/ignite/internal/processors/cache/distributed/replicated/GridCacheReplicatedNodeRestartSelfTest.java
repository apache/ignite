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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheRebalanceMode.SYNC;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 * Tests node restart.
 */
@RunWith(JUnit4.class)
public class GridCacheReplicatedNodeRestartSelfTest extends GridCacheAbstractNodeRestartSelfTest {
    /** {@inheritDoc} */
    @Override protected CacheConfiguration cacheConfiguration() {
        CacheConfiguration cc = defaultCacheConfiguration();

        cc.setNearConfiguration(null);

        cc.setAtomicityMode(atomicityMode());

        cc.setName(CACHE_NAME);

        cc.setCacheMode(REPLICATED);

        cc.setWriteSynchronizationMode(FULL_SYNC);

        cc.setRebalanceMode(SYNC);

        cc.setRebalanceBatchSize(20);

        return cc;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        //outcommented to get failure on TC: fail("https://issues.apache.org/jira/browse/IGNITE-5515");
    }

    /** {@inheritDoc} */
    @Override @Test
    public void testRestartWithPutTwoNodesNoBackups() throws Throwable {
        super.testRestartWithPutTwoNodesNoBackups();
    }

    /** {@inheritDoc} */
    @Override @Test
    public void testRestartWithPutTwoNodesOneBackup() throws Throwable {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override @Test
    public void testRestartWithPutFourNodesOneBackups() throws Throwable {
        super.testRestartWithPutFourNodesOneBackups();
    }

    /** {@inheritDoc} */
    @Override @Test
    public void testRestartWithPutFourNodesNoBackups() throws Throwable {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override @Test
    public void testRestartWithPutSixNodesTwoBackups() throws Throwable {
        super.testRestartWithPutSixNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Override @Test
    public void testRestartWithPutEightNodesTwoBackups() throws Throwable {
        super.testRestartWithPutEightNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Override @Test
    public void testRestartWithPutTenNodesTwoBackups() throws Throwable {
        super.testRestartWithPutTenNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Override @Test
    public void testRestartWithTxTwoNodesNoBackups() throws Throwable {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override @Test
    public void testRestartWithTxTwoNodesOneBackup() throws Throwable {
        super.testRestartWithTxTwoNodesOneBackup();
    }

    /** {@inheritDoc} */
    @Override @Test
    public void testRestartWithTxFourNodesOneBackups() throws Throwable {
        super.testRestartWithTxFourNodesOneBackups();
    }

    /** {@inheritDoc} */
    @Override @Test
    public void testRestartWithTxFourNodesNoBackups() throws Throwable {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override @Test
    public void testRestartWithTxSixNodesTwoBackups() throws Throwable {
        super.testRestartWithTxSixNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Override @Test
    public void testRestartWithTxEightNodesTwoBackups() throws Throwable {
        super.testRestartWithTxEightNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Override @Test
    public void testRestartWithTxTenNodesTwoBackups() throws Throwable {
        super.testRestartWithTxTenNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Override @Test
    public void testRestartWithTxPutAllTenNodesTwoBackups() throws Throwable {
        super.testRestartWithTxPutAllTenNodesTwoBackups();
    }

    /** {@inheritDoc} */
    @Override @Test
    public void testRestartWithTxPutAllFourNodesTwoBackups() throws Throwable {
        super.testRestartWithTxPutAllFourNodesTwoBackups();
    }
}
