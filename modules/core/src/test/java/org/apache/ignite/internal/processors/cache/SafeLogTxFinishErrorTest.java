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

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxAdapter;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxLocalAdapter;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.testframework.GridTestUtils.getFieldValue;
import static org.apache.ignite.testframework.GridTestUtils.setFieldValue;

/**
 * Tests verifying that {@link IgniteTxAdapter#logTxFinishErrorSafe}
 * runs without errors.
 */
public class SafeLogTxFinishErrorTest extends GridCommonAbstractTest {
    /** Logger for listen log messages. */
    private final ListeningTestLogger log = new ListeningTestLogger(false, GridCommonAbstractTest.log);

    /** Flag to remove the FailureHandler when creating configuration for node. */
    private boolean rmvFailureHnd;

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCacheConfiguration(
            new CacheConfiguration<>(DEFAULT_CACHE_NAME)
                .setAffinity(new RendezvousAffinityFunction(false, 32))
                .setAtomicityMode(TRANSACTIONAL)
        );

        if (rmvFailureHnd)
            cfg.setFailureHandler(null);

        return cfg;
    }

    /**
     * Checking that {@link IgniteTxAdapter#logTxFinishErrorSafe} is executable
     * without errors with FailureHandler.
     *
     * @throws Exception If any error occurs.
     */
    @Test
    public void testSafeLogTxFinishErrorWithFailureHandler() throws Exception {
        checkSafeLogTxFinishError(false);
    }

    /**
     * Checking that {@link IgniteTxAdapter#logTxFinishErrorSafe} is executable
     * without errors without FailureHandler.
     *
     * @throws Exception If any error occurs.
     */
    @Test
    public void testSafeLogTxFinishErrorWithoutFailureHandler()throws Exception {
        rmvFailureHnd = true;

        checkSafeLogTxFinishError(false);
    }

    /**
     * Checking that {@link IgniteTxAdapter#logTxFinishErrorSafe} is executable
     * without errors with FailureHandler. Provided that txState field
     * is deleted in transaction, which leads to NPE when
     * {@link GridNearTxLocal#toString()} is called.
     *
     * @throws Exception If any error occurs.
     */
    @Test
    public void testSafeLogTxFinishErrorWithFailureHandlerAndRemoveTxState() throws Exception {
        checkSafeLogTxFinishError(true);
    }

    /**
     * Checking that {@link IgniteTxAdapter#logTxFinishErrorSafe} is executed
     * without errors.
     *
     * @param rmvTxState Remove txState field in transaction.
     * @throws Exception If any error occurs.
     */
    private void checkSafeLogTxFinishError(boolean rmvTxState) throws Exception {
        IgniteEx igniteEx = startGrid(0);

        try (Transaction transaction = igniteEx.transactions().txStart()) {
            IgniteCache<Object, Object> cache = igniteEx.cache(DEFAULT_CACHE_NAME);

            cache.put(1, 1);

            Collection<IgniteInternalTx> activeTxs = igniteEx.context().cache().context().tm().activeTransactions();

            assertEquals(1, activeTxs.size());

            GridNearTxLocal activeTx = (GridNearTxLocal)activeTxs.iterator().next();

            AtomicBoolean containsFailedCompletingTxInLog = new AtomicBoolean();

            Object txState = null;

            if (rmvTxState) {
                txState = getFieldValue(activeTx, IgniteTxLocalAdapter.class, "txState");
                setFieldValue(activeTx, IgniteTxLocalAdapter.class, "txState", null);
            }

            boolean commit = false;

            String errPrefix = "Failed completing the transaction: [commit=" + commit;

            String xidVer = (rmvTxState ? "xidVersion" : "xidVer") + '=' + activeTx.xidVersion();

            log.registerListener(logStr -> {
                if (logStr.startsWith(errPrefix) && logStr.contains(xidVer))
                    containsFailedCompletingTxInLog.set(true);
            });

            activeTx.logTxFinishErrorSafe(log, commit, new RuntimeException("Test"));

            assertTrue(containsFailedCompletingTxInLog.get());

            //That there was no NPE when closing a transaction.
            if (rmvTxState)
                setFieldValue(activeTx, IgniteTxLocalAdapter.class, "txState", txState);
        }
    }
}
