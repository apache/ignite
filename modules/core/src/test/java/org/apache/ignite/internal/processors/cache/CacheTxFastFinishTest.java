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

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.NearCacheConfiguration;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFastFinishFuture;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.internal.processors.cache.transactions.TransactionProxyImpl;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.testframework.MvccFeatureChecker.Feature.NEAR_CACHE;
import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.READ_COMMITTED;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class CacheTxFastFinishTest extends GridCommonAbstractTest {
    /** */
    private boolean nearCache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CacheConfiguration ccfg = new CacheConfiguration(DEFAULT_CACHE_NAME);

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(TRANSACTIONAL);
        ccfg.setBackups(1);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);

        if (nearCache)
            ccfg.setNearConfiguration(new NearCacheConfiguration());

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFastFinishTxNearCache() throws Exception {
        MvccFeatureChecker.skipIfNotSupported(NEAR_CACHE);

        nearCache = true;

        fastFinishTx();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testFastFinishTx() throws Exception {
        fastFinishTx();
    }

    /**
     * @throws Exception If failed.
     */
    protected void fastFinishTx() throws Exception {
        startGrid(0);

        fastFinishTx(ignite(0));

        startClientGrid(1);

        for (int i = 0; i < 2; i++)
            fastFinishTx(ignite(i));

        startGrid(2);

        for (int i = 0; i < 3; i++)
            fastFinishTx(ignite(i));

        startGrid(3);

        for (int i = 0; i < 4; i++)
            fastFinishTx(ignite(i));

        stopGrid(1);

        for (int i = 0; i < 4; i++) {
            if (i != 1)
                fastFinishTx(ignite(i));
        }
    }

    /**
     * @param ignite Node.
     */
    protected void fastFinishTx(Ignite ignite) {
        IgniteTransactions txs = ignite.transactions();

        IgniteCache cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (boolean commit : new boolean[]{true, false}) {
            for (TransactionConcurrency c : TransactionConcurrency.values()) {
                for (TransactionIsolation isolation : TransactionIsolation.values()) {
                    try (Transaction tx = txs.txStart(c, isolation)) {
                        checkFastTxFinish(tx, commit);
                    }
                }
            }

            for (int i = 0; i < 100; i++) {
                try (Transaction tx = txs.txStart(OPTIMISTIC, REPEATABLE_READ)) {
                    cache.get(i);

                    checkFastTxFinish(tx, commit);
                }

                try (Transaction tx = txs.txStart(OPTIMISTIC, READ_COMMITTED)) {
                    cache.get(i);

                    checkFastTxFinish(tx, commit);
                }
            }

            for (int i = 0; i < 100; i++) {
                try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                    cache.get(i);

                    checkNormalTxFinish(tx, commit, true);
                }

                try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.get(i);

                    checkNormalTxFinish(tx, commit, true);
                }
            }

            for (int i = 0; i < 100; i++) {
                for (TransactionConcurrency c : TransactionConcurrency.values()) {
                    for (TransactionIsolation isolation : TransactionIsolation.values()) {
                        try (Transaction tx = txs.txStart(c, isolation)) {
                            cache.put(i, i);

                            checkNormalTxFinish(tx, commit, false);
                        }
                    }
                }
            }
        }
    }

    /**
     * @param tx Transaction.
     * @param commit Commit flag.
     */
    protected void checkFastTxFinish(Transaction tx, boolean commit) {
        if (commit)
            tx.commit();
        else
            tx.rollback();

        IgniteInternalTx tx0 = ((TransactionProxyImpl)tx).tx();

        assertNull(prepareFuture(tx0));
        assertTrue(finishFuture(tx0) instanceof GridNearTxFastFinishFuture);
    }

    /**
     * @param tx Transaction.
     * @param commit Commit flag.
     * @param readOnly {@code true} if checked tx did no writes.
     */
    protected void checkNormalTxFinish(Transaction tx, boolean commit, boolean readOnly) {
        IgniteInternalTx tx0 = ((TransactionProxyImpl)tx).tx();

        if (commit) {
            tx.commit();

            checkNormalCommittedTx(tx0, readOnly);
        }
        else {
            tx.rollback();

            assertNull(prepareFuture(tx0));
            assertNotNull(finishFuture(tx0));
        }
    }

    /** */
    protected void checkNormalCommittedTx(IgniteInternalTx tx, boolean readOnly) {
        assertNotNull(prepareFuture(tx));
        assertNotNull(finishFuture(tx));
    }

    /** */
    protected Object prepareFuture(IgniteInternalTx tx) {
        return fieldValue(tx, "prepFut");
    }

    /** */
    protected Object finishFuture(IgniteInternalTx tx) {
        return fieldValue(tx, "finishFut");
    }

    /**
     * @param obj Obejct.
     * @param fieldName Field name.
     * @return Field value.
     */
    private Object fieldValue(Object obj, String fieldName) {
        Object val = GridTestUtils.getFieldValue(obj, fieldName);

        if (val == null)
            return null;

        if (val instanceof AtomicReference)
            return ((AtomicReference)val).get();

        return val;
    }
}
