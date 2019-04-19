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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.internal.processors.cache.transactions.IgniteInternalTx;
import org.apache.ignite.testframework.MvccFeatureChecker;
import org.apache.ignite.transactions.Transaction;

import static org.apache.ignite.transactions.TransactionConcurrency.PESSIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.REPEATABLE_READ;

/**
 *
 */
public class CacheMvccTxFastFinishTest extends CacheTxFastFinishTest {
    /** {@inheritDoc} */
    @Override protected void fastFinishTx(Ignite ignite) {
        assert MvccFeatureChecker.forcedMvcc();

        IgniteTransactions txs = ignite.transactions();

        IgniteCache<Integer, Integer> cache = ignite.cache(DEFAULT_CACHE_NAME);

        for (boolean commit : new boolean[] {true, false}) {
            try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                checkFastTxFinish(tx, commit);
            }

            for (int i = 0; i < 100; i++) {
                try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.get(i);

                    checkNormalTxFinish(tx, commit, true);
                }
            }

            for (int i = 0; i < 100; i++) {
                try (Transaction tx = txs.txStart(PESSIMISTIC, REPEATABLE_READ)) {
                    cache.put(i, i);

                    checkNormalTxFinish(tx, commit, false);
                }
            }
        }
    }

    /** {@inheritDoc} */
    @Override protected void checkNormalCommittedTx(IgniteInternalTx tx, boolean readOnly) {
        if (readOnly)
            assertNull(prepareFuture(tx));
        else
            assertNotNull(prepareFuture(tx));

        assertNotNull(finishFuture(tx));
    }
}
