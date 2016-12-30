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

package org.apache.ignite.internal.processors.hadoop.impl.shuffle.collections;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.ignite.internal.processors.hadoop.HadoopJobInfo;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInput;
import org.apache.ignite.internal.processors.hadoop.shuffle.collections.HadoopMultimap;
import org.apache.ignite.internal.processors.hadoop.shuffle.collections.HadoopSkipList;
import org.apache.ignite.internal.util.GridRandom;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.io.GridDataInput;
import org.apache.ignite.internal.util.io.GridUnsafeDataInput;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.X;

import static java.lang.Math.abs;
import static java.lang.Math.ceil;
import static java.lang.Math.max;

/**
 * Skip list tests.
 */
public class HadoopSkipListSelfTest extends HadoopAbstractMapTest {
    /**
     *
     */
    public void testLevel() {
        Random rnd = new GridRandom();

        int[] levelsCnts = new int[32];

        int all = 10000;

        for (int i = 0; i < all; i++) {
            int level = HadoopSkipList.randomLevel(rnd);

            levelsCnts[level]++;
        }

        X.println("Distribution: " + Arrays.toString(levelsCnts));

        for (int level = 0; level < levelsCnts.length; level++) {
            int exp = (level + 1) == levelsCnts.length ? 0 : all >>> (level + 1);

            double precission = 0.72 / Math.max(32 >>> level, 1);

            int sigma = max((int)ceil(precission * exp), 5);

            X.println("Level: " + level + " exp: " + exp + " act: " + levelsCnts[level] + " precision: " + precission +
                " sigma: " + sigma);

            assertTrue(abs(exp - levelsCnts[level]) <= sigma); // Sometimes fails.
        }
    }

    /**
     * @throws Exception On error.
     */
    public void testMapSimple() throws Exception {
        GridUnsafeMemory mem = new GridUnsafeMemory(0);

//        mem.listen(new GridOffHeapEventListener() {
//            @Override public void onEvent(GridOffHeapEvent evt) {
//                if (evt == GridOffHeapEvent.ALLOCATE)
//                    U.dumpStack();
//            }
//        });

        Random rnd = new Random();

        int mapSize = 16 << rnd.nextInt(6);

        HadoopJobInfo job = new JobInfo();

        HadoopTaskContext taskCtx = new TaskContext();

        HadoopMultimap m = new HadoopSkipList(job, mem);

        HadoopMultimap.Adder a = m.startAdding(taskCtx);

        Multimap<Integer, Integer> mm = ArrayListMultimap.create();
        Multimap<Integer, Integer> vis = ArrayListMultimap.create();

        for (int i = 0, vals = 4 * mapSize + rnd.nextInt(25); i < vals; i++) {
            int key = rnd.nextInt(mapSize);
            int val = rnd.nextInt();

            a.write(new IntWritable(key), new IntWritable(val));
            mm.put(key, val);

            X.println("k: " + key + " v: " + val);

            a.close();

            check(m, mm, vis, taskCtx);

            a = m.startAdding(taskCtx);
        }

//        a.add(new IntWritable(10), new IntWritable(2));
//        mm.put(10, 2);
//        check(m, mm);

        a.close();

        X.println("Alloc: " + mem.allocatedSize());

        m.close();

        assertEquals(0, mem.allocatedSize());
    }

    /**
     * Check.
     * @param m The multimap.
     * @param mm The multimap storing expectations.
     * @param vis The multimap to store visitor results.
     * @param taskCtx The task context.
     * @throws Exception On error.
     */
    private void check(HadoopMultimap m, Multimap<Integer, Integer> mm, final Multimap<Integer, Integer> vis,
        HadoopTaskContext taskCtx)
        throws Exception {
        final HadoopTaskInput in = m.input(taskCtx);

        Map<Integer, Collection<Integer>> mmm = mm.asMap();

        int keys = 0;

        int prevKey = Integer.MIN_VALUE;

        while (in.next()) {
            keys++;

            IntWritable k = (IntWritable)in.key();

            assertNotNull(k);

            assertTrue(k.get() > prevKey);

            prevKey = k.get();

            Deque<Integer> vs = new LinkedList<>();

            Iterator<?> it = in.values();

            while (it.hasNext())
                vs.addFirst(((IntWritable) it.next()).get());

            Collection<Integer> exp = mmm.get(k.get());

            assertEquals(exp, vs);
        }

        assertEquals(mmm.size(), keys);

//!        assertEquals(m.keys(), keys);

        // Check visitor.

        final byte[] buf = new byte[4];

        final GridDataInput dataInput = new GridUnsafeDataInput();

        m.visit(false, new HadoopMultimap.Visitor() {
            /** */
            IntWritable key = new IntWritable();

            /** */
            IntWritable val = new IntWritable();

            @Override public void onKey(long keyPtr, int keySize) {
                read(keyPtr, keySize, key);
            }

            @Override public void onValue(long valPtr, int valSize) {
                read(valPtr, valSize, val);

                vis.put(key.get(), val.get());
            }

            private void read(long ptr, int size, Writable w) {
                assert size == 4 : size;

                GridUnsafe.copyOffheapHeap(ptr, buf, GridUnsafe.BYTE_ARR_OFF, size);

                dataInput.bytes(buf, size);

                try {
                    w.readFields(dataInput);
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

//        X.println("vis: " + vis);

        assertEquals(mm, vis);

        in.close();
    }

    /**
     * @throws Exception if failed.
     */
    public void testMultiThreaded() throws Exception {
        GridUnsafeMemory mem = new GridUnsafeMemory(0);

        X.println("___ Started");

        Random rnd = new GridRandom();

        for (int i = 0; i < 20; i++) {
            HadoopJobInfo job = new JobInfo();

            final HadoopTaskContext taskCtx = new TaskContext();

            final HadoopMultimap m = new HadoopSkipList(job, mem);

            final ConcurrentMap<Integer, Collection<Integer>> mm = new ConcurrentHashMap<>();

            X.println("___ MT");

            multithreaded(new Callable<Object>() {
                @Override public Object call() throws Exception {
                    X.println("___ TH in");

                    Random rnd = new GridRandom();

                    IntWritable key = new IntWritable();
                    IntWritable val = new IntWritable();

                    HadoopMultimap.Adder a = m.startAdding(taskCtx);

                    for (int i = 0; i < 50000; i++) {
                        int k = rnd.nextInt(32000);
                        int v = rnd.nextInt();

                        key.set(k);
                        val.set(v);

                        a.write(key, val);

                        Collection<Integer> list = mm.get(k);

                        if (list == null) {
                            list = new ConcurrentLinkedQueue<>();

                            Collection<Integer> old = mm.putIfAbsent(k, list);

                            if (old != null)
                                list = old;
                        }

                        list.add(v);
                    }

                    a.close();

                    X.println("___ TH out");

                    return null;
                }
            }, 3 + rnd.nextInt(27));

            HadoopTaskInput in = m.input(taskCtx);

            int prevKey = Integer.MIN_VALUE;

            while (in.next()) {
                IntWritable key = (IntWritable)in.key();

                assertTrue(key.get() > prevKey);

                prevKey = key.get();

                Iterator<?> valsIter = in.values();

                Collection<Integer> vals = mm.remove(key.get());

                assertNotNull(vals);

                while (valsIter.hasNext()) {
                    IntWritable val = (IntWritable) valsIter.next();

                    assertTrue(vals.remove(val.get()));
                }

                assertTrue(vals.isEmpty());
            }

            in.close();
            m.close();

            assertEquals(0, mem.allocatedSize());
        }
    }
}