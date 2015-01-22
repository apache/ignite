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

package org.gridgain.grid.lang;

import org.apache.ignite.internal.util.typedef.internal.*;
import org.pcollections.*;

import java.util.*;

/**
 *
 */
public class GridImmutableCollectionsPerfomanceTest {
    /** */
    private static final Random RND = new Random();

    /** */
    private static final int ITER_CNT = 100000;

    /**
     * @param args Args.
     */
    public static void main(String[] args) {
        for (int i = 0; i < 20; i++) {
            for (int j = 0; j < 5; j++) {
                // testArrayList();

                testPVector();

                testPVectorRemove();
            }
        }
    }

    /**
     *
     */
    private static void testArrayList() {
        System.gc();

        Collection<Integer> list = new ArrayList<>();

        long start = U.currentTimeMillis();

        for (int i = 0; i < ITER_CNT; i++) {
            Integer[] arr = new Integer[list.size() + 1];

            list.toArray(arr);

            arr[arr.length - 1] = RND.nextInt(100000);

            Collection<Integer> cp = Arrays.asList(arr);

            assert cp.size() - 1 == list.size();

            list = cp;
        }

        assert list.size() == ITER_CNT;

        System.out.println("Array list time: " + (U.currentTimeMillis() - start));
    }

    /**
     *
     */
    private static void testPVector() {
        System.gc();

        long start = U.currentTimeMillis();

        TreePVector<Integer> list = TreePVector.empty();

        for (int i = 0; i < ITER_CNT; i++) {
            TreePVector<Integer> cp = list.plus(RND.nextInt(100000));

            assert cp.size() - 1 == list.size();

            list = cp;
        }

        assert list.size() == ITER_CNT;

        System.out.println("testPVector time: " + (U.currentTimeMillis() - start));
    }

    /**
     *
     */
    private static void testPVectorRemove() {
        System.gc();

        long start = U.currentTimeMillis();

        TreePVector<Integer> list = TreePVector.empty();

        for (int i = 0; i < ITER_CNT; i++)
            list = list.plus(RND.nextInt(100));

        for (int i = 0; i < ITER_CNT; i++)
            list = list.minus(new Integer(RND.nextInt(100)));

        System.out.println("testPVectorRemove time: " + (U.currentTimeMillis() - start));
    }
}
