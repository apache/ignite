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

package org.apache.ignite.internal.processors.hadoop.shuffle.collections;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import org.apache.hadoop.io.IntWritable;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskContext;
import org.apache.ignite.internal.processors.hadoop.HadoopTaskInput;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.X;

/**
 *
 */
public class HadoopHashMapSelfTest extends HadoopAbstractMapTest {

    public void testAllocation() throws Exception {
        fail("https://issues.apache.org/jira/browse/IGNITE-826");

        final GridUnsafeMemory mem = new GridUnsafeMemory(0);

        long size = 3L * 1024 * 1024 * 1024;

        final long chunk = 16;// * 1024;

        final int page = 4 * 1024;

        final int writes = chunk < page ? 1 : (int)(chunk / page);

        final long cnt = size / chunk;

        assert cnt < Integer.MAX_VALUE;

        final int threads = 4;

        long start = System.currentTimeMillis();

        multithreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int cnt0 = (int)(cnt / threads);

                for (int i = 0; i < cnt0; i++) {
                    long ptr = mem.allocate(chunk);

                    for (int j = 0; j < writes; j++)
                        mem.writeInt(ptr + j * page, 100500);
                }

                return null;
            }
        }, threads);

        X.println("End: " + (System.currentTimeMillis() - start) + " mem: " + mem.allocatedSize() + " cnt: " + cnt);

        Thread.sleep(30000);
    }


    /** */
    public void testMapSimple() throws Exception {
        GridUnsafeMemory mem = new GridUnsafeMemory(0);

//        mem.listen(new GridOffHeapEventListener() {
//            @Override public void onEvent(GridOffHeapEvent evt) {
//                if (evt == GridOffHeapEvent.ALLOCATE)
//                    U.dumpStack();
//            }
//        });

        Random rnd = new Random();

        int mapSize = 16 << rnd.nextInt(3);

        HadoopTaskContext taskCtx = new TaskContext();

        final HadoopHashMultimap m = new HadoopHashMultimap(new JobInfo(), mem, mapSize);

        HadoopMultimap.Adder a = m.startAdding(taskCtx);

        Multimap<Integer, Integer> mm = ArrayListMultimap.create();

        for (int i = 0, vals = 4 * mapSize + rnd.nextInt(25); i < vals; i++) {
            int key = rnd.nextInt(mapSize);
            int val = rnd.nextInt();

            a.write(new IntWritable(key), new IntWritable(val));
            mm.put(key, val);

            X.println("k: " + key + " v: " + val);

            a.close();

            check(m, mm, taskCtx);

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

    private void check(HadoopHashMultimap m, Multimap<Integer, Integer> mm, HadoopTaskContext taskCtx) throws Exception {
        final HadoopTaskInput in = m.input(taskCtx);

        Map<Integer, Collection<Integer>> mmm = mm.asMap();

        int keys = 0;

        while (in.next()) {
            keys++;

            IntWritable k = (IntWritable)in.key();

            assertNotNull(k);

            ArrayList<Integer> vs = new ArrayList<>();

            Iterator<?> it = in.values();

            while (it.hasNext())
                vs.add(((IntWritable) it.next()).get());

            Collection<Integer> exp = mmm.get(k.get());

            assertEquals(sorted(exp), sorted(vs));
        }

        X.println("keys: " + keys + " cap: " + m.capacity());

        assertEquals(mmm.size(), keys);

        assertEquals(m.keys(), keys);

        in.close();
    }

    private GridLongList sorted(Collection<Integer> col) {
        GridLongList lst = new GridLongList(col.size());

        for (Integer i : col)
            lst.add(i);

        return lst.sort();
    }
}