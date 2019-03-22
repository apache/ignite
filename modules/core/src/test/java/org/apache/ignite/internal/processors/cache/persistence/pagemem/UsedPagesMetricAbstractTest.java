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
     * @param nodeCount count of ignite nodes
     * @param iterations count of check iterations
     * @param storedEntriesCount count of key-value pairs in each iteration
     * @param valueSize size of value in bytes
     * @throws Exception
     */
    protected void testFillAndRemove(
            int nodeCount,
            int iterations,
            int storedEntriesCount,
            int valueSize
    ) throws Exception {
        Ignite node = startGrids(nodeCount);
        node.cluster().active(true);
        IgniteCache cache = node.getOrCreateCache(MY_CACHE);

        long beforeFill;
        long afterFill;
        long afterRemove;

        for (int iter = 0; iter < iterations; iter++) {

            DataRegionMetrics metricsBeforeFill = node.dataRegionMetrics(DEFAULT_DATA_REGION);
            beforeFill = metricsBeforeFill.getTotalUsedPages();

            for (int i = 0; i < storedEntriesCount; i++) {
                final long res = (i * i) % LARGE_PRIME;
                cache.put(res, new byte[valueSize]);
            }

            DataRegionMetrics metricsAfterFill = node.dataRegionMetrics(DEFAULT_DATA_REGION);
            afterFill = metricsAfterFill.getTotalUsedPages();

            for (int i = 0; i < storedEntriesCount; i++) {
                final long res = (i * i) % LARGE_PRIME;
                cache.remove(res);
            }

            DataRegionMetrics metricsAfterRemove = node.dataRegionMetrics(DEFAULT_DATA_REGION);
            afterRemove = metricsAfterRemove.getTotalUsedPages();

            log.info(String.format("Used pages count before fill: %d", beforeFill));
            log.info(String.format("Used pages count after fill: %d", afterFill));
            log.info(String.format("Used pages count after remove: %d\n", afterRemove));

            assertTrue(afterFill > beforeFill);
            assertTrue(afterRemove < afterFill);
            assertTrue(afterRemove >= beforeFill);
        }
    }
}
