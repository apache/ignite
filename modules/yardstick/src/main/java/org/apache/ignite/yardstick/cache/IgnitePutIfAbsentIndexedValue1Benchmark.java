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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.yardstick.cache.model.Person1;

/**
 * Ignite benchmark that performs putIfAbsent operations for entity with indexed fields.
 */
public class IgnitePutIfAbsentIndexedValue1Benchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** */
    private final AtomicInteger insCnt = new AtomicInteger();

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        IgniteCache<Integer, Object> cache = cacheForOperation();

        int key = insCnt.getAndIncrement();

        cache.putIfAbsent(key, new Person1(key));

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("atomic-index-with-eviction");
    }
}
