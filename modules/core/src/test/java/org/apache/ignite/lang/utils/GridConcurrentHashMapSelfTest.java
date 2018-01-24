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
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ThreadLocalRandom8;

/**
 * Tests for {@link ConcurrentHashMap}.
 */
public class GridConcurrentHashMapSelfTest extends GridCommonAbstractTest {
    /** */
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private Map<Integer,Integer> map;

    /**
     * @throws Exception If failed.
     */
    public void testOpsSpeed() throws Exception {
        for (int i = 0; i < 4; i++) {
            map = new ConcurrentHashMap<>();

            info("Map ops time: " + runOps(1000000, 100));
        }
    }

    /**
     * @param iterCnt Iterations count.
     * @param threadCnt Threads count.
     * @return Time taken.
     */
    @SuppressWarnings("SameParameterValue")
    private long runOps(final int iterCnt, int threadCnt) throws Exception {
        long start = System.currentTimeMillis();

        multithreaded(() -> {
            ThreadLocalRandom8 rnd = ThreadLocalRandom8.current();

            for (int i = 0; i < iterCnt; i++) {
                // Put random.
                map.put(rnd.nextInt(0, 10000), 0);

                // Read random.
                map.get(rnd.nextInt(0, 10000));

                // Remove random.
                map.remove(rnd.nextInt(0, 10000));
            }

            return null;
        }, threadCnt);

        return System.currentTimeMillis() - start;
    }

    /** */
    public void testCreationTime() {
        for (int i = 0; i < 5; i++) {
            long now = System.currentTimeMillis();

            for (int j = 0; j < 1000000; j++)
                new ConcurrentHashMap<Integer, Integer>();

            info("Map creation time: " + (System.currentTimeMillis() - now));
        }
    }
}