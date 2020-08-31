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

package org.apache.ignite.internal.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * GridConcurrentMultiPairQueue test.
 **/
public class GridConcurrentMultiPairQueueTest extends GridCommonAbstractTest {
    /** */
    GridConcurrentMultiPairQueue<Integer, Integer> queue;

    /** */
    GridConcurrentMultiPairQueue<Integer, Integer> queue2;

    /** */
    Map<Integer, Collection<Integer>> mapForCheck;

    /** */
    Map<Integer, Collection<Integer>> mapForCheck2;

    /** */
    Integer[] arr2 = {2, 4};

    /** */
    Integer[] arr1 = {1, 3, 5, 7, 9, 11, 13, 15, 17, 19};

    /** */
    Integer[] arr4 = {};

    /** */
    Integer[] arr5 = {};

    /** */
    Integer[] arr3 = {100, 200, 300, 400, 500, 600, 600, 700};

    /** */
    Integer[] arr6 = {};

    /** */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        Collection<T2<Integer, Integer[]>> keyWithArr = new HashSet<>();

        mapForCheck = new ConcurrentHashMap<>();

        mapForCheck2 = new ConcurrentHashMap<>();

        keyWithArr.add(new T2<>(10, arr2));
        keyWithArr.add(new T2<>(20, arr1));
        keyWithArr.add(new T2<>(30, arr4));
        keyWithArr.add(new T2<>(40, arr5));
        keyWithArr.add(new T2<>(50, arr3));
        keyWithArr.add(new T2<>(60, arr6));

        mapForCheck.put(10, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr2))));
        mapForCheck.put(20, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr1))));
        mapForCheck.put(50, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr3))));

        mapForCheck2.put(10, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr2))));
        mapForCheck2.put(20, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr1))));
        mapForCheck2.put(50, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr3))));

        queue = new GridConcurrentMultiPairQueue<>(keyWithArr);

        Map<Integer, Collection<Integer>> keyWithColl = new HashMap<>();

        keyWithColl.put(10, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr2))));
        keyWithColl.put(20, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr1))));
        keyWithColl.put(30, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr4))));
        keyWithColl.put(40, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr5))));
        keyWithColl.put(50, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr3))));
        keyWithColl.put(60, Collections.synchronizedCollection(new ArrayList<>(Arrays.asList(arr6))));

        queue2 = new GridConcurrentMultiPairQueue<>(keyWithColl);
    }

    /** */
    @Test
    public void testGridConcurrentMultiPairQueueCorrectness() throws Exception {
        GridTestUtils.runMultiThreaded(() -> {
            GridConcurrentMultiPairQueue.Result<Integer, Integer> res =
                new GridConcurrentMultiPairQueue.Result<>();

            while (queue.next(res)) {
                assertTrue(mapForCheck.containsKey(res.getKey()));

                assertTrue(mapForCheck.get(res.getKey()).remove(res.getValue()));

                Collection<Integer> coll = mapForCheck.get(res.getKey());

                if (coll != null && coll.isEmpty())
                    mapForCheck.remove(res.getKey(), coll);
            }
        }, ThreadLocalRandom.current().nextInt(1, 20), "GridConcurrentMultiPairQueue arr test");

        assertTrue(mapForCheck.isEmpty());

        assertTrue(queue.isEmpty());

        assertTrue(queue.initialSize() == arr1.length + arr2.length + arr3.length + arr4.length);

        GridTestUtils.runMultiThreaded(() -> {
            GridConcurrentMultiPairQueue.Result<Integer, Integer> res =
                new GridConcurrentMultiPairQueue.Result<>();

            while (queue2.next(res)) {
                assertTrue(mapForCheck2.containsKey(res.getKey()));

                assertTrue(mapForCheck2.get(res.getKey()).remove(res.getValue()));

                Collection<Integer> coll = mapForCheck2.get(res.getKey());

                if (coll != null && coll.isEmpty())
                    mapForCheck2.remove(res.getKey(), coll);
            }
        }, ThreadLocalRandom.current().nextInt(1, 20), "GridConcurrentMultiPairQueue coll test");

        assertTrue(mapForCheck2.isEmpty());

        assertTrue(queue2.isEmpty());

        assertTrue(queue2.initialSize() == arr1.length + arr2.length + arr3.length + arr4.length);
    }
}
