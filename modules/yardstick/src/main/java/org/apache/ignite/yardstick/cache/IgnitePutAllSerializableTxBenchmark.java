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

package org.apache.ignite.yardstick.cache;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionOptimisticException;
import org.yardstickframework.BenchmarkConfiguration;

import static org.apache.ignite.transactions.TransactionConcurrency.OPTIMISTIC;
import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 *
 */
public class IgnitePutAllSerializableTxBenchmark extends IgniteCacheAbstractBenchmark {
    /** */
    private IgniteTransactions txs;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        txs = ignite().transactions();
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        ThreadRange r = threadRange();

        Map<Integer, Integer> vals = new HashMap<>();

        for (int i = 0; i < args.batch(); i++) {
            int key = r.nextRandom();

            vals.put(key, key);
        }

        IgniteCache<Integer, Object> cache = cacheForOperation();

        while (true) {
            try (Transaction tx = txs.txStart(OPTIMISTIC, SERIALIZABLE)) {
                cache.putAll(vals);

                tx.commit();
            }
            catch (TransactionOptimisticException e) {
                continue;
            }

            break;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("tx");
    }
}
