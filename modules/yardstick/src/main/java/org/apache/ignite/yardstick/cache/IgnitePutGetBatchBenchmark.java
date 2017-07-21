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
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.yardstick.cache.model.SampleValue;

/**
 * Ignite benchmark that performs batch put and get operations.
 */
public class IgnitePutGetBatchBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        IgniteCache<Integer, Object> cache = cacheForOperation();

        Set<Integer> keys = new TreeSet<>();

        while (keys.size() < args.batch())
            keys.add(nextRandom(args.range()));

        Map<Integer, Object> vals = cache.getAll(keys);

        Map<Integer, SampleValue> updates = new TreeMap<>();

        for (Integer key : keys) {
            Object val = vals.get(key);

            if (val != null)
                key = nextRandom(args.range());

            updates.put(key, new SampleValue(key));
        }

        cache.putAll(updates);

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("atomic");
    }
}
