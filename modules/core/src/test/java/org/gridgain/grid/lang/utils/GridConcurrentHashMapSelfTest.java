/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang.utils;

import org.gridgain.grid.util.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

/**
 * Tests for {@link ConcurrentHashMap8}.
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
