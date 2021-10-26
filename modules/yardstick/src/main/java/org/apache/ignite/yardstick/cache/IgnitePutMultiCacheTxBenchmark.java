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
import java.util.UUID;
import java.util.concurrent.Callable;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.yardstick.IgniteBenchmarkUtils;
import org.yardstickframework.BenchmarkConfiguration;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;

/** Multi cache transactional put benchmark. */
public class IgnitePutMultiCacheTxBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** */
    private IgniteTransactions txs;

    /** */
    private Callable<Void> clo;

    /** Num of cache operations.*/
    private int cacheOperations;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        if (cachesCnt() <= 1)
            throw new IllegalArgumentException("Please configure --cachesCnt" +
                " param, need to be more that 1.");

        if (!IgniteSystemProperties.getBoolean("SKIP_MAP_CHECK"))
            ignite().compute().broadcast(new WaitMapExchangeFinishCallable());

        txs = ignite().transactions();

        cacheOperations = args.opsPerCache();

        for (IgniteCache<?, ?> cache : testCaches) {
            if (cache.getConfiguration(CacheConfiguration.class).getAtomicityMode() != TRANSACTIONAL)
                throw new IllegalArgumentException("Only transactional caches need to be present.");
        }

        clo = () -> {
            int key = nextRandom(args.range());

            int shift = 0;

            for (int i = 0; i < cacheOperations; ++i) {
                for (IgniteCache cache : testCaches) {
                    cache.put(key, new SampleValue(key + shift, UUID.randomUUID()));
                    ++shift;
                }
            }

            return null;
        };
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        IgniteBenchmarkUtils.doInTransaction(txs, args.txConcurrency(), args.txIsolation(), clo);

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("tx");
    }

    /** */
    private static class SampleValue {
        /** */
        @QuerySqlField
        private int id;

        /** */
        @QuerySqlField
        private UUID uid;

        /** */
        private SampleValue() {
            // No-op.
        }

        /** */
        public SampleValue(int id, UUID uid) {
            this.id = id;
            this.uid = uid;
        }
    }
}
