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

package org.gridgain.grid.lang.utils;

import com.romix.scala.collection.concurrent.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

/**
 * Tests for {@code TrieMap}.
 */
public class GridTrieMapSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int OPS_CNT = 1000000;

    /** */
    private static final int MAIN_ITER_CNT = 5;

    /** */
    private static final int THREAD_CNT = Runtime.getRuntime().availableProcessors();

    /** */
    private Map<Integer,Integer> map;

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testOpsSpeed() throws Exception {
        info("Test operations without clone: ");

        for (int i = 0; i < MAIN_ITER_CNT; i++) {
            map = new TrieMap<>();

            info("Trie map ops time: " + runOps(OPS_CNT, THREAD_CNT, null));

            map = new ConcurrentHashMap8<>();

            info("New map ops time: " + runOps(OPS_CNT, THREAD_CNT, null));

            map = new ConcurrentHashMap<>();

            info("Jdk6 map ops time: " + runOps(OPS_CNT, THREAD_CNT, null));
        }

        info("Test operations with clone: ");

        for (int i = 0; i < MAIN_ITER_CNT; i++) {
            map = new TrieMap<>();

            info("Trie map ops time: " + runOps(OPS_CNT, THREAD_CNT, new C1<Map, Map>() {
                @Override public Map apply(Map e) {
                    return ((TrieMap)e).readOnlySnapshot();
                }
            }));

            map = new ConcurrentHashMap8<>();

            info("New map ops time: " + runOps(OPS_CNT, THREAD_CNT, new C1<Map, Map>() {
                @Override public Map apply(Map e) {
                    return new ConcurrentHashMap8(e);
                }
            }));

            map = new ConcurrentHashMap<>();

            info("Jdk6 map ops time: " + runOps(OPS_CNT, THREAD_CNT, new C1<Map, Map>() {
                @Override public Map apply(Map e) {
                    return new ConcurrentHashMap(e);
                }
            }));
        }
    }

    /**
     *
     */
    public void _testClear() {
        new TrieMap<>().clear();
    }

    /**
     * @param iterCnt Iterations count.
     * @param threadCnt Threads count.
     * @param cloner Cloner.
     * @return Time taken.
     * @throws Exception If failed.
     */
    private long runOps(final int iterCnt, int threadCnt, @Nullable final IgniteClosure<Map, Map> cloner)
        throws Exception {
        long start = System.currentTimeMillis();

        final ReadWriteLock lock = cloner != null ? new ReentrantReadWriteLock() : null;
        final GridTuple<Integer> lower = F.t(0);
        final AtomicBoolean cloneGuard = cloner != null ? new AtomicBoolean() : null;

        multithreaded(new Callable<Object>() {
            @SuppressWarnings("TooBroadScope")
            @Override public Object call() throws Exception {
                boolean clone = cloneGuard != null && cloneGuard.compareAndSet(false, true);

                ThreadLocalRandom8 rnd = ThreadLocalRandom8.current();

                for (int i = 0; i < iterCnt; i++) {
                    int lowerBound;

                    readLock(lock);

                    try {
                        lowerBound = lower.get();

                        // Put random.
                        map.put(rnd.nextInt(lowerBound, lowerBound + 10000), 0);

                        // Read random.
                        map.get(rnd.nextInt(lowerBound, lowerBound + 10000));

                        // Remove random.
                        map.remove(rnd.nextInt(lowerBound, lowerBound + 10000));
                    }
                    finally {
                        readUnlock(lock);
                    }

                    if (clone && i % 1000 == 0) {
                        Map map0;

                        writeLock(lock);

                        try {
                            map0 = cloner.apply(map);

                            lower.set(lowerBound + 20000);
                        }
                        finally {
                            writeUnlock(lock);
                        }

                        for (Object o : map0.keySet()) {
                            int j = (Integer)o;

                            assert j <= lowerBound + 10000;
                        }
                    }
                }

                return null;
            }
        }, threadCnt);

        return System.currentTimeMillis() - start;
    }

    /**
     * @param lock Lock.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    private void readLock(@Nullable ReadWriteLock lock) {
        if (lock == null)
            return;

        lock.readLock().lock();
    }

    /**
     * @param lock Lock.
     */
    private void readUnlock(@Nullable ReadWriteLock lock) {
        if (lock == null)
            return;

        lock.readLock().unlock();
    }

    /**
     * @param lock Lock.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    private void writeLock(@Nullable ReadWriteLock lock) {
        lock.writeLock().lock();
    }

    /**
     * @param lock Lock.
     */
    private void writeUnlock(@Nullable ReadWriteLock lock) {
        lock.writeLock().unlock();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ResultOfObjectAllocationIgnored")
    public void testCreationTime() throws Exception {
        for (int i = 0; i < 5; i++) {
            long now = System.currentTimeMillis();

            for (int j = 0; j < 1000000; j++)
                new TrieMap<Integer, Integer>();

            info("Trie map creation time: " + (System.currentTimeMillis() - now));

            now = System.currentTimeMillis();

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
