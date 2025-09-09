/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.yardstick.cache;

import java.util.Map;
import java.util.function.Consumer;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.yardstick.IgniteBenchmarkUtils;
import org.yardstickframework.BenchmarkConfiguration;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/** Ignite benchmark that performs transactional putAll multi cache operations.*/
public class IgnitePutAllMultiCacheTxBenchmark extends IgnitePutAllBenchmark {
    /** */
    private Consumer<Map<Integer, Integer>> cons;

    /** */
    private IgniteTransactions txs;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        txs = ignite().transactions();

        if (cachesCnt() <= 1)
            throw new IllegalArgumentException("Please configure --cachesCnt" +
                " param, need to be more that 1.");

        for (IgniteCache<?, ?> cache : testCaches) {
            if (cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() != TRANSACTIONAL)
                throw new IllegalArgumentException("Only transactional caches need to be present.");
        }

        cons = (vals) -> {
            for (IgniteCache cache : testCaches)
                cache.putAll(vals);
        };
    }

    /** Put operations.*/
    @Override protected void putData(Map<Integer, Integer> vals) throws Exception {
        IgniteBenchmarkUtils.doInTransaction(txs, args.txConcurrency(), args.txIsolation(),
            () -> {
                cons.accept(vals);
                return null;
            });
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("tx");
    }
}
