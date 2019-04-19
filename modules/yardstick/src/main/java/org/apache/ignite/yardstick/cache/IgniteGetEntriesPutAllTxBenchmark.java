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

package org.apache.ignite.yardstick.cache;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.yardstickframework.BenchmarkConfiguration;

import static org.apache.ignite.yardstick.IgniteBenchmarkUtils.doInTransaction;

/**
 * Ignite benchmark that performs transactional putAll operations.
 */
public class IgniteGetEntriesPutAllTxBenchmark extends IgniteCacheAbstractBenchmark<Integer, Integer> {
    /** */
    private IgniteTransactions txs;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        txs = ignite().transactions();
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        final ThreadRange r = threadRange();

        doInTransaction(txs, args.txConcurrency(), args.txIsolation(), new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteCache<Integer, Integer> cache = cacheForOperation();

                SortedMap<Integer, Integer> vals = new TreeMap<>();

                for (int i = 0; i < args.batch(); i++) {
                    int key = r.nextRandom();

                    vals.put(key, key);
                }

                cache.getEntries(vals.keySet());

                cache.putAll(vals);

                return null;
            }
        });

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Integer> cache() {
        return ignite().cache("tx");
    }
}
