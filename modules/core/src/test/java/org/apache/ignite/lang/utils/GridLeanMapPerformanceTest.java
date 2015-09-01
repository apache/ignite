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

package org.apache.ignite.lang.utils;

import java.util.Map;
import org.apache.ignite.internal.util.GridLeanMap;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * Performance test for {@link GridLeanMap}.
 */
public class GridLeanMapPerformanceTest extends GridCommonAbstractTest {
    /** */
    private static final int RUN_CNT = 5;

    /** */
    private static final int ITER_CNT = 5 * 1000 * 1000;

    /**
     * @throws Exception If failed.
     */
    public void testPerformance() throws Exception {
        long avgDur = 0;

        for (int i = 0; i <= RUN_CNT; i++) {
            long start = System.currentTimeMillis();

            Map<Integer, Integer> map = new GridLeanMap<>(0);

            for (int j = 0; j < 5; j++) {
                map.put(i, i);

                iterate(map);
            }

            long dur = System.currentTimeMillis() - start;

            info("Run " + i + (i == 0 ? " (warm up)" : "") + ": " + dur + "ms.");

            if (i > 0)
                avgDur += dur;
        }

        avgDur /= 5;

        info("Average (excluding warm up): " + avgDur + "ms.");
    }

    /**
     * Iterates through map collections.
     *
     * @param map Map.
     * @throws Exception In case of error.
     */
    @SuppressWarnings({"StatementWithEmptyBody", "UnusedDeclaration"})
    private void iterate(Map<Integer, Integer> map) throws Exception {
        for (int i = 1; i <= ITER_CNT; i++) {
            // Iterate through entries.
            for (Map.Entry<Integer, Integer> e : map.entrySet());

            // Iterate through keys.
            for (Integer k : map.keySet());

            // Iterate through values.
            for (Integer v : map.values());
        }
    }
}