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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.ThreadLocalRandom8;

/**
 * Tests for {@link org.jsr166.ConcurrentHashMap8}.
 */
public class GridConcurrentHashMapSelfTest extends GridCommonAbstractTest {
    /** */
    private Map<Integer,Integer> map;

    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        map = new ConcurrentHashMap8<>();

        map.put(0, 0);
        map.put(0, 0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOpsSpeed() throws Exception {
        for (int i = 0; i < 4; i++) {
            map = new ConcurrentHashMap8<>();

            info("New map ops time: " + runOps(1000000, 100));

            map = new ConcurrentHashMap<>();

            info("Jdk6 map ops time: " + runOps(1000000, 100));
        }
    }

    /**
     * @param iterCnt Iterations count.
     * @param threadCnt Threads count.
     * @return Time taken.
     */
    private long runOps(final int iterCnt, int threadCnt) throws Exception {
        long start = System.currentTimeMillis();

        multithreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
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
            }
        }, threadCnt);

        return System.currentTimeMillis() - start;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    public void testCreationTime() throws Exception {
        for (int i = 0; i < 5; i++) {
            long now = System.currentTimeMillis();

            for (int j = 0; j < 1000000; j++)
                new ConcurrentHashMap8<Integer, Integer>();

            info("New map creation time: " + (System.currentTimeMillis() - now));

            now = System.currentTimeMillis();

            for (int j = 0; j < 1000000; j++)
                new ConcurrentHashMap<Integer, Integer>();

            info("Jdk6 map creation time: " + (System.currentTimeMillis() - now));
        }
    }
}