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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.yardstick.cache.model.SampleValue;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Ignite benchmark that performs transactional put operations skipping key if local node is backup.
 */
public class IgnitePutTxPrimaryOnlyBenchmark extends IgniteCacheAbstractBenchmark {
    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        if (!IgniteSystemProperties.getBoolean("SKIP_MAP_CHECK"))
            ignite().compute().broadcast(new WaitMapExchangeFinishCallable());
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        IgniteCache<Integer, Object> cache = cacheForOperation();

        int key;

        Affinity<Object> aff = ignite().affinity("tx");
        ClusterNode locNode = ignite().cluster().localNode();

        for (;;) {
            key = nextRandom(args.range());

            // Exit only if primary.
            if (aff.isPrimary(locNode, key))
                break;
        }

        // Implicit transaction is used.
        cache.put(key, new SampleValue(key));

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("tx");
    }
}
