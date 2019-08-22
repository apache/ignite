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

import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.yardstickframework.BenchmarkConfiguration;

import static org.apache.ignite.yardstick.IgniteBenchmarkUtils.doInTransaction;

/**
 * Ignite benchmark that performs transactional putAll operations.
 */
public class IgniteGetAllPutAllTxBytesKeyBenchmark extends IgniteCacheAbstractBenchmark<byte[], Integer> {
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

        doInTransaction(txs, args.txConcurrency(), args.txIsolation(), () -> {
            SortedMap<byte[], Integer> vals = new TreeMap<>(Comparator.comparing(String::new));

            for (int i = 0; i < args.batch(); i++) {
                int base = r.nextRandom();

                byte[] key = String.valueOf(base).getBytes();

                vals.put(key, base);
            }

            IgniteCache<byte[], Integer> cache = cacheForOperation();

            cache.getAll(vals.keySet());

            cache.putAll(vals);

            return null;
        });

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<byte[], Integer> cache() {
        return ignite().cache("tx");
    }
}
