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

import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.yardstickframework.BenchmarkConfiguration;

/**
 *
 */
public class IgniteScanQueryBenchmark extends IgniteCacheAbstractBenchmark<Integer, Object> {
    /** {@inheritDoc} */
    @Override public void setUp(BenchmarkConfiguration cfg) throws Exception {
        super.setUp(cfg);

        loadCachesData();
    }

    /** {@inheritDoc} */
    @Override protected void loadCacheData(String cacheName) {
        loadSampleValues(cacheName, args.range());
    }

    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int key = nextRandom(args.range());

        ScanQuery<Integer, Object> qry = new ScanQuery<>();

        qry.setFilter(new KeyFilter(key));

        IgniteCache<Integer, Object> cache = cacheForOperation().withKeepBinary();

        List<IgniteCache.Entry<Integer, Object>> res = cache.query(qry).getAll();

        if (res.size() != 1)
            throw new Exception("Invalid result size: " + res.size());

        if (res.get(0).getKey() != key)
            throw new Exception("Invalid entry found [key=" + key + ", entryKey=" + res.get(0).getKey() + ']');

        return true;
    }

    /** {@inheritDoc} */
    @Override protected IgniteCache<Integer, Object> cache() {
        return ignite().cache("atomic");
    }

    /**
     *
     */
    static class KeyFilter implements IgniteBiPredicate<Integer, Object> {
        /** */
        private final Integer key;

        /**
         * @param key Key to find.
         */
        public KeyFilter(Integer key) {
            this.key = key;
        }

        /** {@inheritDoc} */
        @Override public boolean apply(Integer key, Object val) {
            return this.key.equals(key);
        }
    }
}
