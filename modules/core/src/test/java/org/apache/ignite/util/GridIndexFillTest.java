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

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.util.GridConcurrentSkipListSet;
import org.apache.ignite.internal.util.snaptree.SnapTreeMap;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 * TODO write doc
 */
public class GridIndexFillTest extends GridCommonAbstractTest {
    /** */
    private CopyOnWriteArrayList<Idx> idxs;

    /** */
    private ConcurrentHashMap<Integer, CountDownLatch> keyLocks;

    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        idxs = new CopyOnWriteArrayList<>();

        idxs.add(new Idx(true));

        keyLocks = new ConcurrentHashMap<>();
    }

    /**
     * @param k Key.
     */
    private CountDownLatch lock(String op, Integer k) {
//        U.debug(op + " lock: " + k);
        CountDownLatch latch = new CountDownLatch(1);

        for(;;) {
            CountDownLatch l = keyLocks.putIfAbsent(k, latch);

            if (l == null)
                return latch;

            try {
                l.await();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * @param k Key.
     */
    private void unlock(Integer k, CountDownLatch latch) {
//        U.debug("unlock: " + k);
        assertTrue(keyLocks.remove(k, latch));

        latch.countDown();
    }

    private void put(Integer k, Long v) {
        CountDownLatch l = lock("add", k);

        for (Idx idx : idxs)
            idx.add(k, v);

        unlock(k, l);
    }

    private void remove(Integer k) {
        CountDownLatch l = lock("rm", k);

        try {
            Long v = null;

            for (Idx idx : idxs) {
                Long v2 = idx.remove(k, v);

                if (v2 == null) {
                    assert v == null;

                    return; // Nothing to remove.
                }

                if (v == null)
                    v = v2;
                else
                    assert v.equals(v2);
            }
        }
        finally {
            unlock(k, l);
        }
    }

    public void testSnaptreeParallelBuild() throws Exception {
        final AtomicBoolean stop = new AtomicBoolean();

        IgniteInternalFuture<?> fut = multithreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                ThreadLocalRandom rnd = ThreadLocalRandom.current();

                while (!stop.get()) {
                    int k = rnd.nextInt(100);
                    long v = rnd.nextLong(10);

                    if (rnd.nextBoolean())
                        put(k, v);
                    else
                        remove(k);
                 }

                return null;
            }
        }, 12, "put");

        Thread.sleep(500);

        Idx newIdx = new Idx(false);

        idxs.add(newIdx);

        SnapTreeMap<Integer, Long> snap = idxs.get(0).tree.clone();

        for (Map.Entry<Integer, Long> entry : snap.entrySet())
            newIdx.addX(entry.getKey(), entry.getValue());

        newIdx.finish();

        stop.set(true);

        fut.get();

        assertEquals(idxs.get(0).tree, idxs.get(1).tree);
    }

    private static class Idx {

        static int z = 1;

        private final SnapTreeMap<Integer, Long> tree = new SnapTreeMap<>(); //new ConcurrentSkipListMap<>();

        private volatile Rm rm;

        private final String name = "idx" + z++;

        public Idx(boolean pk) {
            if (!pk)
                rm = new Rm();
        }

        public void add(Integer k, Long v) {
//            U.debug(name + " add: k" + k + " " + v);

            Long old = tree.put(k, v);

            if (old != null) {
                Rm rm = this.rm;

                if (rm != null)
                    rm.keys.add(k);
            }
        }

        public void addX(Integer k, Long v) {
//            U.debug(name + " addX: k" + k + " " + v);

            assert v != null;
            assert k != null;

//            Lock l = rm.lock.writeLock();

//            l.lock();

            try {
                if (!rm.keys.contains(k)) {
//                    U.debug(name + " addX-put: k" + k + " " + v);

                    tree.putIfAbsent(k, v);
                }
            }
            finally {
//                l.unlock();
            }
        }

        public Long remove(Integer k, Long v) {
            Rm rm = this.rm;

            if (rm != null) {
                assert v != null;

//                Lock l = rm.lock.readLock();

//                l.lock();

                try {
                    rm.keys.add(k);

                    Long v2 = tree.remove(k);

//                    U.debug(name + " rm1: k" + k + " " + v + " " + v2);

                }
                finally {
//                    l.unlock();
                }
            }
            else {
                Long v2 = tree.remove(k);

//                U.debug(name + " rm2: k" + k + " " + v + " " + v2);

                if (v == null)
                    v = v2;
                else
                    assertEquals(v, v2);
            }

            return v;
        }

        public void finish() {
//            assertTrue(rm.tree.isEmpty());

            rm = null;
        }
    }

    private static class Rm {
//        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        private final GridConcurrentSkipListSet<Integer> keys = new GridConcurrentSkipListSet<>();
            //new SnapTreeMap<>(); //new ConcurrentSkipListMap<>();
    }
}