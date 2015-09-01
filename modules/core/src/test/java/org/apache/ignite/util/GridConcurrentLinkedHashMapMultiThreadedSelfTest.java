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

package org.apache.ignite.util;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentLinkedHashMap;

import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.PER_SEGMENT_Q;
import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.PER_SEGMENT_Q_OPTIMIZED_RMV;
import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.SINGLE_Q;

/**
 *
 */
public class GridConcurrentLinkedHashMapMultiThreadedSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    public void testPut() throws Exception {
        info(">>> Test grid concurrent linked hash map...");

        int keyCnt = 1000000;

        ConcurrentLinkedHashMap<Integer, String> linkedMap =
            new ConcurrentLinkedHashMap<>(1000, 0.75f, 64);

        putMultiThreaded(linkedMap, 10, keyCnt, 0);

        assert linkedMap.size() == keyCnt;
        assert linkedMap.sizex() == keyCnt;
        assert linkedMap.queue().sizex() == keyCnt;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPutPerSegment() throws Exception {
        info(">>> Test grid concurrent linked hash map...");

        int keyCnt = 1000000;

        ConcurrentLinkedHashMap<Integer, String> linkedMap =
            new ConcurrentLinkedHashMap<>(1000, 0.75f, 64, 0, SINGLE_Q);

        putMultiThreaded(linkedMap, 10, keyCnt, 0);

        assert linkedMap.size() == keyCnt;
        assert linkedMap.sizex() == keyCnt;
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvict() throws Exception {
        info(">>> Test grid concurrent linked hash map...");

        final int maxSize = 1000;

        ConcurrentLinkedHashMap<Integer, String> linkedMap = new ConcurrentLinkedHashMap<>(
            32, 0.75f, 64, maxSize);

        int keyCnt = 1000000;

        int diff = 10; // 1% of 1000.

        Map<String, LinkedList<Integer>> map = putMultiThreaded(linkedMap, 10, keyCnt, maxSize);

        LinkedList<Integer> keys = new LinkedList<>(linkedMap.keySet());

        assertTrue("Invalid key set size: " + keys.size(), U.safeAbs(maxSize - keys.size()) <= diff);

        assertTrue("Invalid map size: " + linkedMap.size(), U.safeAbs(maxSize - linkedMap.size()) <= diff);
        assertTrue("Invalid map sizex: " + linkedMap.sizex(), U.safeAbs(maxSize - linkedMap.sizex()) <= diff);
        assertTrue("Invalid map queue size: " + linkedMap.queue().sizex(),
            U.safeAbs(maxSize - linkedMap.queue().sizex()) <= diff);

        while (!keys.isEmpty()) {
            boolean found = false;

            int key = keys.removeLast();

            for (LinkedList<Integer> threadKeys : map.values()) {
                if (threadKeys.getLast() == key) {
                    threadKeys.removeLast();

                    found = true;

                    break;
                }
            }

            assertTrue("Key was not found on the top of any thread: " + key, found);
        }

        int min = Integer.MAX_VALUE;
        int max = 0;

        int actKeyCnt = 0;

        for (int key : linkedMap.keySet()) {
            min = Math.min(min, key);
            max = Math.max(max, key);

            actKeyCnt++;
        }

        info("Max: " + max);
        info("Min: " + min);

        assertTrue("Invalid keys count: " + actKeyCnt, U.safeAbs(maxSize - actKeyCnt) <= diff);
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvictPerSegment() throws Exception {
        info(">>> Test grid concurrent linked hash map...");

        final int maxSize = 1000;

        ConcurrentLinkedHashMap<Integer, String> linkedMap = new ConcurrentLinkedHashMap<>(
            32, 0.75f, 64, maxSize, PER_SEGMENT_Q);

        int keyCnt = 1000000;

        putMultiThreaded(linkedMap, 10, keyCnt, maxSize);

        int diff = 10; // 1% of 1000.

        assertTrue("Invalid map size: " + linkedMap.size(), U.safeAbs(maxSize - linkedMap.size()) <= diff);
        assertTrue("Invalid map sizex: " + linkedMap.sizex(), U.safeAbs(maxSize - linkedMap.sizex()) <= diff);

//      TODO IGNITE-606 - Need to fix iterators for ConcurrentLinkedHashMap in perSegment mode
//        LinkedList<Integer> keys = new LinkedList<Integer>(linkedMap.keySet());
//
//        while (!keys.isEmpty()) {
//            boolean found = false;
//
//            int key = keys.removeLast();
//
//            for (LinkedList<Integer> threadKeys : map.values()) {
//                if (threadKeys.getLast() == key) {
//                    threadKeys.removeLast();
//
//                    found = true;
//
//                    break;
//                }
//            }
//
//            assertTrue("Key was not found on the top of any thread: " + key, found);
//        }

        int min = Integer.MAX_VALUE;
        int max = 0;

        int actKeyCnt = 0;

        for (int key = 0; key < keyCnt; key++) {
            if (linkedMap.get(key) != null) {
                min = Math.min(min, key);
                max = Math.max(max, key);

                actKeyCnt++;
            }
        }

        info("Max: " + max);
        info("Min: " + min);

        assertTrue("Invalid keys count: " + actKeyCnt, U.safeAbs(maxSize - actKeyCnt) <= diff);
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvictPerSegmentOptimizedRemoves() throws Exception {
        info(">>> Test grid concurrent linked hash map...");

        final int maxSize = 1000;

        ConcurrentLinkedHashMap<Integer, String> linkedMap = new ConcurrentLinkedHashMap<>(
            32, 0.75f, 64, maxSize, PER_SEGMENT_Q_OPTIMIZED_RMV);

        int keyCnt = 1000000;

        putMultiThreaded(linkedMap, 10, keyCnt, maxSize);

        int diff = 10; // 1% of 1000.

        assertTrue("Invalid map size: " + linkedMap.size(), U.safeAbs(maxSize - linkedMap.size()) <= diff);
        assertTrue("Invalid map sizex: " + linkedMap.sizex(), U.safeAbs(maxSize - linkedMap.sizex()) <= diff);

//      TODO IGNITE-606 - Need to fix iterators for ConcurrentLinkedHashMap in perSegment mode
//        LinkedList<Integer> keys = new LinkedList<Integer>(linkedMap.keySet());
//
//        while (!keys.isEmpty()) {
//            boolean found = false;
//
//            int key = keys.removeLast();
//
//            for (LinkedList<Integer> threadKeys : map.values()) {
//                if (threadKeys.getLast() == key) {
//                    threadKeys.removeLast();
//
//                    found = true;
//
//                    break;
//                }
//            }
//
//            assertTrue("Key was not found on the top of any thread: " + key, found);
//        }

        int min = Integer.MAX_VALUE;
        int max = 0;

        int actKeyCnt = 0;

        for (int key = 0; key < keyCnt; key++) {
            if (linkedMap.get(key) != null) {
                min = Math.min(min, key);
                max = Math.max(max, key);

                actKeyCnt++;
            }
        }

        info("Max: " + max);
        info("Min: " + min);

        assertTrue("Invalid keys count: " + actKeyCnt, U.safeAbs(maxSize - actKeyCnt) <= diff);
    }

    /**
     * @param map       Map.
     * @param threadCnt Thread count.
     * @param keyCnt    Key count.
     * @param rememberCnt Maximum count of added keys per thread to remember
     * @return Keys distribution across threads.
     * @throws Exception If failed.
     */
    private Map<String, LinkedList<Integer>> putMultiThreaded(final ConcurrentMap<Integer, String> map, int threadCnt,
        final int keyCnt, final int rememberCnt)
        throws Exception {
        final AtomicInteger keyGen = new AtomicInteger();

        long start = U.currentTimeMillis();
        final ConcurrentHashMap<String, LinkedList<Integer>> res = new ConcurrentHashMap<>();

        // Producer thread.
        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    String thNm = Thread.currentThread().getName();

                    LinkedList<Integer> keys = new LinkedList<>();

                    LinkedList<Integer> old = res.put(thNm, keys);

                    assert old == null;

                    while (true) {
                        Integer key = keyGen.getAndIncrement();

                        if (key >= keyCnt)
                            break;

                        map.put(key, "value");
                        keys.add(key);

                        if (keys.size() > rememberCnt)
                            keys.removeFirst();
                    }

                    return null;
                }
            },
            threadCnt,
            "producer"
        );

        fut.get();

        info("Put finished [keyCnt=" + keyCnt + ", threadCnt=" + threadCnt +
            ", duration=" + (U.currentTimeMillis() - start) + ']');

        return res;
    }

    /**
     * @throws Exception If failed.
     */
    public void testInsertOrderIterator() throws Exception {
        final AtomicBoolean run = new AtomicBoolean(true);

        info(">>> Test grid concurrent linked hash map iterator...");

        final Map<Integer, String> linkedMap =
            new ConcurrentLinkedHashMap<>();

        Set<Integer> original = new HashSet<>();

        final int keyCnt = 10000;

        for (int i = 0; i < keyCnt; i++) {
            linkedMap.put(i, "value" + i);
            original.add(i);
        }

        long start = System.currentTimeMillis();

        // Updater threads.
        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    Random rnd = new Random();

                    while (run.get()) {
                        int key = rnd.nextInt(keyCnt);

                        linkedMap.put(key, "value" + key);
                    }

                    return null;
                }
            },
            10,
            "updater"
        );

        try {
            // Check that iterator always contains all the values.
            int iterCnt = 10000;

            for (int i = 0; i < iterCnt; i++) {
                Collection<Integer> cp = new HashSet<>(original);

                cp.removeAll(linkedMap.keySet());

                assertTrue("Keys disappeared from map: " + cp, cp.isEmpty());
            }

            info(">>> Iterator test complete [duration = " + (System.currentTimeMillis() - start) + ']');
        }
        finally {
            run.set(false);
            fut.get();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteratorModificationInsertOrder() throws Exception {
        testGetRemovePutIterator();
    }

    /**
     * @throws Exception If failed.
     */
    public void testInsertOrderGetRemovePut() throws Exception {
        testPutGetRemove(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testInsertOrderPutGetRemove() throws Exception {
        testPutGetRemove(true);
    }

    /**
     * Test multithreaded put, get, remove operations in concurrent linked hash map.
     *
     *
     * @param clear {@code true} if test should expect clear map at the end.
     * @throws Exception If failed.
     */
    private void testPutGetRemove(final boolean clear) throws Exception {
        info(">>> Test grid concurrent linked hash map iterator...");

        final ConcurrentLinkedHashMap<Integer, String> linkedMap =
            new ConcurrentLinkedHashMap<>();

        Collection<Integer> original = new HashSet<>();

        final int keyCnt = 10000;

        if (!clear)
            for (int i = 0; i < keyCnt; i++) {
                linkedMap.put(i, "value" + i);
                original.add(i);
            }

        long start = System.currentTimeMillis();

        // Updater threads.
        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    Random rnd = new Random();

                    int iterCnt = 100000;

                    for (int i = 0; i < iterCnt; i++) {
                        int key = rnd.nextInt(keyCnt);

                        if (clear) {
                            linkedMap.put(key, "value" + key);

                            linkedMap.get(key);

                            linkedMap.remove(key);
                        }
                        else {
                            linkedMap.get(key);

                            linkedMap.remove(key);

                            linkedMap.put(key, "value" + key);
                        }
                    }

                    return null;
                }
            },
            10,
            "updater"
        );

        fut.get();

        Set<Integer> keys = linkedMap.keySet();

        if (clear)
            assertTrue("Keys must not be in map " + keys, keys.isEmpty());
        else {
            original.removeAll(keys);
            assertTrue("Keys must be in map: " + original, original.isEmpty());
        }

        info(">>> put get remove test complete [duration = " + (System.currentTimeMillis() - start) + ']');
    }


    /**
     * Test multithreaded operations in concurrent linked hash map and iterator consistency.
     *
     * @throws Exception If failed.
     */
    public void testGetRemovePutIterator() throws Exception {

        info(">>> Test grid concurrent linked hash map iterator...");

        final ConcurrentLinkedHashMap<Integer, String> linkedMap =
            new ConcurrentLinkedHashMap<>();

        Collection<Integer> original = new HashSet<>();

        final int keyCnt = 10000;

        for (int i = 0; i < keyCnt; i++) {
            linkedMap.put(i, "value" + i);
            original.add(i);
        }

        final AtomicBoolean run = new AtomicBoolean(true);

        long start = System.currentTimeMillis();

        // Updater threads.
        IgniteInternalFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    Random rnd = new Random();

                    while (run.get()) {
                        int key = rnd.nextInt(keyCnt);

                        linkedMap.get(key);

                        linkedMap.remove(key);

                        linkedMap.put(key, "value" + key);
                    }

                    info(">>> Exiting updater thread");

                    return null;
                }
            },
            10,
            "updater"
        );

        int iterCnt = 10000;

        for (int i = 0; i < iterCnt; i++) {
            Iterator<Integer> it = linkedMap.keySet().iterator();

            Collection<Integer> keys = new HashSet<>();

            // Since we have 10 running threads, iterator should show not less then keyCnt - 10 elements.
            while (it.hasNext()) {
                int key = it.next();

                assertFalse("Duplicate key: " + key, keys.contains(key));
                keys.add(key);
            }

            if (i % 500 == 0)
                info(">>> Run "  + i + " iterations in " + (System.currentTimeMillis() - start) + "ms");
        }

        info(">>> Stopping updater threads");

        run.set(false);

        fut.get();

        info(">>> Updater threads stopped, will verify integrity of result map");

        Set<Integer> keys = linkedMap.keySet();

        original.removeAll(keys);
        assertTrue("Keys must be in map: " + original, original.isEmpty());

        info(">>> put get remove test complete [duration = " + (System.currentTimeMillis() - start) + ']');
    }
}