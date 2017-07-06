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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.U;
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

    /** */
    private int srvrCnt;

    /** */
    private int stripesCnt;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        aff = ignite().affinity(cache().getName());

        Collection<ClusterNode> nodes = ignite().cluster().forServers().nodes();

        stripesCnt = ignite().cluster().forServers().forRandom().metrics().getTotalCpus();

        srvrCnt = nodes.size();

        IgniteLogger log = ignite().log();

        if (log.isInfoEnabled())
            log.info("Servers info [srvrsCnt=" + srvrCnt + ", stripesCnt=" + stripesCnt + ']');
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        List<Map<Integer, Integer>> putMaps = (List<Map<Integer, Integer>>)ctx.get(PUT_MAPS_KEY);

        if (putMaps == null) {
            putMaps = new ArrayList<>(PUT_MAPS_CNT);

            ctx.put(PUT_MAPS_KEY, putMaps);
        }

        Map<Integer, Integer> vals;

        if (putMaps.size() == PUT_MAPS_CNT)
            vals = putMaps.get(nextRandom(PUT_MAPS_CNT));
        else {
            vals = new TreeMap<>();

            ClusterNode node = args.collocated() ? aff.mapKeyToNode(nextRandom(args.range())) : null;

            Map<ClusterNode, Integer> stripesMap = null;

            if (args.singleStripe())
                stripesMap = U.newHashMap(srvrCnt);

            for (; vals.size() < args.batch(); ) {
                int key = nextRandom(args.range());

                if (args.collocated() && !aff.isPrimary(
                    node,
                    key))
                    continue;

                if (args.singleStripe()) {
                    int part = aff.partition(key);

                    ClusterNode node0 = node != null ? node : aff.mapPartitionToNode(part);

                    Integer stripe0 = stripesMap.get(node0);
                    int stripe = part % stripesCnt;

                    if (stripe0 != null) {
                        if (stripe0 != stripe)
                            continue;
                    }
                    else
                        stripesMap.put(
                            node0,
                            stripe);
                }

                vals.put(
                    key,
                    key);
            }

            putMaps.add(vals);

            if (putMaps.size() == PUT_MAPS_CNT) {
                IgniteLogger log = ignite().log();

                if (log.isInfoEnabled())
                    log.info("Put maps set generated.");
            }
        }

        IgniteCache<Integer, Object> cache = cacheForOperation();

        cache.putAll(vals);

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("atomic");
    }
}
