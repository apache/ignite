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

import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.yardstick.IgniteBenchmarkUtils;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Ignite benchmark that performs get operation inside transaction.
 */
public class IgniteGetTxExplicitBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** IgniteTransactions. */
    private IgniteTransactions txs;

    /** Get operation callable. */
    private Callable<Void> call;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        if (args.preloadAmount() > args.range())
            throw new IllegalArgumentException("Preloading amount (\"-pa\", \"--preloadAmount\") " +
                "must by less then the range (\"-r\", \"--range\").");

        loadCachesData();

        txs = ignite().transactions();

        call = new Callable<Void>() {
            @Override public Void call() throws Exception {
                IgniteCache<Integer, Object> cache = cacheForOperation();

                int key = nextRandom(args.range());

                cache.get(key);

                return null;
            }
        };
    }

    /** {@inheritDoc} */
    @Override protected void loadCacheData(String cacheName) {
        loadSampleValues(cacheName, args.preloadAmount());
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        IgniteBenchmarkUtils.doInTransaction(txs, args.txConcurrency(), args.txIsolation(), call);

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("tx");
    }
}


