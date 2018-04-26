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

package org.apache.ignite.yardstick.thin.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.yardstick.cache.IgniteCacheAbstractBenchmark;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Ignite benchmark that performs putAll operations.
 */
public class IgniteThinPutAllBenchmark extends IgniteThinCacheAbstractBenchmark<Integer, Object> {
    /** */
    private static final Integer PUT_MAPS_KEY = 2048;

    /** */
    private static final Integer PUT_MAPS_CNT = 256;

    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        Map<Integer, Integer> vals = new HashMap<>();

        for (int i = 0; i < 500; i++ )
            vals.put(i, i);

        cache().putAll(vals);

        return true;
    }

    /** {@inheritDoc} */
    @Override protected ClientCache<Integer, Object> cache() {
        return client().cache("atomic");
    }
}
