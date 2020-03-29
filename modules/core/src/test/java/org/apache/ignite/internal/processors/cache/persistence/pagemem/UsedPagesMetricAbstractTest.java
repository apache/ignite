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
package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import org.apache.ignite.DataRegionMetrics;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Abstract class for TotalUsedPages metric tests.
 */
public class UsedPagesMetricAbstractTest extends GridCommonAbstractTest {
    /** */
    public static final String MY_CACHE = "myCache";

    /** */
    public static final String DEFAULT_DATA_REGION = "default";

    /** */
    public static final long LARGE_PRIME = 4294967291L;

    /**
     * Common scenario for used pages metric test
     *
     * @param nodeCnt count of ignite nodes
     * @param iterations count of check iterations
     * @param storedEntriesCnt count of key-value pairs in each iteration
     * @param valSize size of value in bytes
     * @throws Exception if failed
     */
    protected void testFillAndRemove(
        int nodeCnt,
        int iterations,
        int storedEntriesCnt,
        int valSize
    ) throws Exception {
        Ignite node = startGrids(nodeCnt);

        node.cluster().active(true);

        IgniteCache<Long, Object> cache = node.getOrCreateCache(MY_CACHE);

        long beforeFill;
        long afterFill;
        long afterRmv;

        for (int iter = 0; iter < iterations; iter++) {

            DataRegionMetrics metricsBeforeFill = node.dataRegionMetrics(DEFAULT_DATA_REGION);

            beforeFill = metricsBeforeFill.getTotalUsedPages();

            for (int i = 0; i < storedEntriesCnt; i++) {
                final long res = (i * i) % LARGE_PRIME;

                cache.put(res, new byte[valSize]);
            }

            DataRegionMetrics metricsAfterFill = node.dataRegionMetrics(DEFAULT_DATA_REGION);

            afterFill = metricsAfterFill.getTotalUsedPages();

            for (int i = 0; i < storedEntriesCnt; i++) {
                final long res = (i * i) % LARGE_PRIME;

                cache.remove(res);
            }

            DataRegionMetrics metricsAfterRmv = node.dataRegionMetrics(DEFAULT_DATA_REGION);

            afterRmv = metricsAfterRmv.getTotalUsedPages();

            log.info(String.format("Used pages count before fill: %d", beforeFill));
            log.info(String.format("Used pages count after fill: %d", afterFill));
            log.info(String.format("Used pages count after remove: %d\n", afterRmv));

            assertTrue(afterFill > beforeFill);
            assertTrue(afterRmv < afterFill);
            assertTrue(afterRmv >= beforeFill);
        }
    }
}
