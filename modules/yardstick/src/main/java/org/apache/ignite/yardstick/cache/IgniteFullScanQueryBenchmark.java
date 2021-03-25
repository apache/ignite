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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.query.ScanQuery;
import org.yardstickframework.BenchmarkUtils;

/**
 *
 */
public class IgniteFullScanQueryBenchmark extends IgniteScanQueryBenchmark {
    /** {@inheritDoc} */
    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        IgniteCache<Integer, Object> cache = cacheForOperation().withKeepBinary();

        AtomicReference<Integer> ref = new AtomicReference<>();

        long start0 = System.currentTimeMillis();

        cache.query(new ScanQuery<Integer, Object>().setDataPageScanEnabled(true))
            .forEach(e -> ref.set(e.getKey()));

        long stop0 = System.currentTimeMillis();


        long start1 = System.currentTimeMillis();

        cache.query(new ScanQuery<Integer, Object>().setDataPageScanEnabled(false))
            .forEach(e -> ref.set(e.getKey()));

        long stop1 = System.currentTimeMillis();

        long x = stop0 - start0;
        long y = stop1 - start1;

        BenchmarkUtils.println(">>>>>> IgniteFullScanQueryBenchmark [data_page_scan=" + x +
            " ms, tree_page_scan=" + y + " ms, percentage=" + calc(x, y) + "%");

        return true;
    }

    /**
     * @param x First.
     * @param y Second.
     * @return Percentage.
     */
    private static String calc(float x, float y) {
        float res = ((x - y)/((x + y) / 2)) * 100;

        return String.format("%.2f", res);
    }

    /** {@inheritDoc} */
    @Override protected void loadCacheData(String cacheName) {
        super.loadCacheData(cacheName);
    }
}
