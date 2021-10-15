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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Ignite benchmark that performs putAll operations.
 */
public class IgnitePutAllBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** */
    private static final Integer PUT_MAPS_KEY = 2048;

    /** */
    private static final Integer PUT_MAPS_CNT = 256;

    /** Affinity mapper. */
    private Affinity<Integer> aff;

    /** Sequentially grow thread data identifier.*/
    AtomicInteger threadIdent = new AtomicInteger();

    /** Predefined batches.*/
    List<Map<Integer, Map<Integer, Integer>>> batchMaps;

    /** {@inheritDoc} */
    @Override public void tearDown() throws Exception {
        super.tearDown();

        if (threadIdent.get() != batchMaps.size())
            throw new IgniteException("Some workers are not initialized.");
    }

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        int threadsCnt = cfg.threads();

        batchMaps = new ArrayList<>(threadsCnt);

        for (int i = 0; i < threadsCnt; ++i)
            batchMaps.add(null);

        for (int i = 0; i < threadsCnt; ++i) {
            for (int m = 0; m < PUT_MAPS_CNT; ++m) {
                TreeMap<Integer, Integer> vals = new TreeMap<>();

                ClusterNode node = args.collocated() ? aff.mapKeyToNode(nextRandom(args.range())) : null;

                for (; vals.size() < args.batch(); ) {
                    int key = nextRandom(args.range());
                    
                    if (args.collocated() && !aff.isPrimary(node, key))
                        continue;

                    vals.put(key, key);
                }

                Map<Integer, Map<Integer, Integer>> map = batchMaps.get(i);

                if (map == null)
                    batchMaps.set(i, map = new HashMap<>());

                map.put(m, vals);
            }
        }

        aff = ignite().affinity(cache().getName());

        IgniteLogger log = ignite().log();

        if (log.isInfoEnabled())
            log.info("Initialization completed, batches predefined for " + threadsCnt + "threads.");
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        if (ctx.isEmpty()) {
            int currCeil = threadIdent.getAndIncrement();

            Map<Integer, Map<Integer, Integer>> batches = batchMaps.get(currCeil);

            ctx.put(PUT_MAPS_KEY, batches);
        }

        Map<Integer, Map<Integer, Integer>> batches = (Map<Integer, Map<Integer, Integer>>)ctx.get(PUT_MAPS_KEY);

        Map<Integer, Integer> vals = batches.get(nextRandom(PUT_MAPS_CNT));

        putData(vals);

        return true;
    }
    
    /** Put operations.*/
    protected void putData(Map<Integer, Integer> vals) throws Exception {
        IgniteCache<Integer, Object> cache = cacheForOperation();

        cache.putAll(vals);
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("atomic");
    }
}
