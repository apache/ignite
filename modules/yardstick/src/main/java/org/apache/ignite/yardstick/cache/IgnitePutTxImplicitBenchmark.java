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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.yardstick.cache.model.SampleValue;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Ignite benchmark that performs transactional put operations using implicit transaction.
 */
public class IgnitePutTxImplicitBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        if (!IgniteSystemProperties.getBoolean("SKIP_MAP_CHECK"))
            ignite().compute().broadcast(new WaitMapExchangeFinishCallable());
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        IgniteCache<Integer, Object> cache = cacheForOperation();

        int key = nextRandom(args.range());

        // Implicit transaction is used.
        cache.put(key, new SampleValue(key));

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("tx");
    }
}
