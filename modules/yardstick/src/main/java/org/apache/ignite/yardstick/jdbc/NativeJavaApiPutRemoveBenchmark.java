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

package org.apache.ignite.yardstick.jdbc;

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.yardstickframework.BenchmarkConfiguration;

/**
 * Benchmark that fetches data from cache to compare with SQL SELECT operation.
 */
public class NativeJavaApiPutRemoveBenchmark extends AbstractNativeBenchmark {
    /** Cache for created table. */
    private IgniteCache<Object, Object> tabCache;

    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        tabCache = ignite().cache("SQL_PUBLIC_TEST_LONG");
    }

    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        long insertKey = ThreadLocalRandom.current().nextLong(args.range()) + 1 + args.range();
        long insertVal = insertKey + 1;

        try {
            tabCache.put(insertKey, insertVal);
            tabCache.remove(insertKey);
        } catch (IgniteException ign){
            // Collision occurred, ignoring.
        }

        return true;
    }
}
