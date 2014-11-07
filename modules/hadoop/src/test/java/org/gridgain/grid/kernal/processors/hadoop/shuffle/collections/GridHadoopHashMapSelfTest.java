/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.hadoop.shuffle.collections;

import com.google.common.collect.*;
import org.apache.hadoop.io.*;
import org.gridgain.grid.hadoop.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.offheap.unsafe.*;
import org.gridgain.grid.util.typedef.*;

import java.util.*;
import java.util.concurrent.*;

/**
 *
 */
public class GridHadoopHashMapSelfTest extends GridHadoopAbstractMapTest {

    public void _testAllocation() throws Exception {
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

        GridHadoopJob job = mockJob();

        GridHadoopTaskContext taskCtx = mockTaskContext(job);

        final GridHadoopHashMultimap m = new GridHadoopHashMultimap(job, mem, mapSize);

        GridHadoopMultimap.Adder a = m.startAdding(taskCtx);

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

    private void check(GridHadoopHashMultimap m, Multimap<Integer, Integer> mm, GridHadoopTaskContext taskCtx) throws Exception {
        final GridHadoopTaskInput in = m.input(taskCtx);

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
