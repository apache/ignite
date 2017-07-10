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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.cache.GridCacheAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearCacheAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxManager;
import org.apache.ignite.internal.util.lang.GridAbsPredicate;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 *
 */
public abstract class AbstractTransactionsInMultipleThreadsTest extends GridCommonAbstractTest {
    /** Transaction isolation level. */
    protected TransactionIsolation transactionIsolation;

    /** Id of node, started transaction. */
    protected int txInitiatorNodeId = 0;

    /**
     * Creates new cache configuration.
     *
     * @return CacheConfiguration New cache configuration.
     */
    protected CacheConfiguration<String, Integer> getCacheConfiguration() {
        CacheConfiguration<String, Integer> cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(CacheMode.PARTITIONED);

        return cacheCfg;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClientMode(false);
        cfg.setCacheConfiguration(getCacheConfiguration());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        checkAllTransactionsHasEnded();
    }

    /**
     * Checks whether all transactions has ended.
     */
    private void checkAllTransactionsHasEnded() {
        for (Ignite ignite : G.allGrids()) {
            GridCacheSharedContext<Object, Object> cctx = ((IgniteKernal)ignite).context().cache().context();

            IgniteTxManager txMgr = cctx.tm();

            assertTrue(txMgr.activeTransactions().isEmpty());
        }
    }

    /**
     * Starts test scenario for all transaction isolation levels.
     *
     * @param testScenario Test scenario.
     * @throws Exception If scenario failed.
     */
    protected void runWithAllIsolations(IgniteCallable<Void> testScenario) throws Exception {
        for (TransactionIsolation isolation : TransactionIsolation.values()) {
            this.transactionIsolation = isolation;

            try {
                testScenario.call();
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }
    }

    /**
     * Waits all transactions to finish.
     *
     * @throws IgniteInterruptedCheckedException If interrupted.
     */
    protected void waitAllTransactionsHasFinished() throws IgniteInterruptedCheckedException {
        boolean txFinished = GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                GridCacheAdapter<?, ?> cache = ((IgniteKernal)ignite(0)).internalCache(DEFAULT_CACHE_NAME);

                IgniteTxManager txMgr = cache.isNear() ?
                    ((GridNearCacheAdapter)cache).dht().context().tm() :
                    cache.context().tm();

                return txMgr.activeTransactions().isEmpty();
            }
        }, 10000);

        assertTrue(txFinished);
    }
}
