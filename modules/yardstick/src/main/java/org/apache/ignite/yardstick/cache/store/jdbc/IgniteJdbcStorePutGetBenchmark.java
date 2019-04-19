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

package org.apache.ignite.yardstick.cache.store.jdbc;

import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.yardstick.cache.model.SampleKey;
import org.apache.ignite.yardstick.cache.model.SampleValue;

/**
 * Ignite benchmark that performs put and get operations.
 */
public class IgniteJdbcStorePutGetBenchmark extends IgniteJdbcStoreAbstractBenchmark {
    /** {@inheritDoc} */
    @Override protected int fillRange() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int id = nextRandom(args.range());

        Object val = cache.get(new SampleKey(id));

        if (val != null)
            id = nextRandom(args.range());

        cache.put(new SampleKey(id), new SampleValue(id));

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Object, Object> cache() {
        return ignite().cache("atomic");
    }
}