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
import org.apache.ignite.IgniteCache;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Ignite benchmark that performs getAll operations.
 */
public class IgniteGetAllTxBenchmark extends IgniteGetBenchmark {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        Set<Integer> keys = U.newHashSet(args.batch());

        while (keys.size() < args.batch()) {
            int key = nextRandom(args.range());

            keys.add(key);
        }

        IgniteCache<Integer, Object> cache = cacheForOperation();

        cache.getAll(keys);

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("tx");
    }
}
