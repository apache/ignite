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
import org.jdk8.backport.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 *
 */
public class GridCircularBufferSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public void testCreation() {
        try {
            GridCircularBuffer<Integer> buf = new GridCircularBuffer<>(-2);

            assert false;

            info("Created buffer: " + buf);
        }
        catch (Exception e) {
            info("Caught expected exception: " + e);
        }

        try {
            GridCircularBuffer<Integer> buf = new GridCircularBuffer<>(0);

            assert false;

            info("Created buffer: " + buf);
        }
        catch (Exception e) {
            info("Caught expected exception: " + e);
        }

        try {
            GridCircularBuffer<Integer> buf = new GridCircularBuffer<>(5);

            assert false;

            info("Created buffer: " + buf);
        }
        catch (Exception e) {
            info("Caught expected exception: " + e);
        }

        GridCircularBuffer<Integer> buf = new GridCircularBuffer<>(8);

        info("Created buffer: " + buf);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleThreaded() throws Exception {
        int size = 8;
        int iterCnt = size * 10;

        GridCircularBuffer<Integer> buf = new GridCircularBuffer<>(size);

        info("Created buffer: " + buf);

        Integer lastEvicted = null;

        for (int i = 0; i < iterCnt; i++) {
            Integer evicted = buf.add(i);

            info("Evicted: " + evicted);

            if (i >= size) {
                assert evicted != null;

                if (lastEvicted == null) {
                    lastEvicted = evicted;

                    continue;
                }

                assert lastEvicted + 1 == evicted : "Fail [lastEvicted=" + lastEvicted + ", evicted=" + evicted + ']';

                lastEvicted = evicted;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testMutliThreaded() throws Exception {
        int size = 32 * 1024;

        final GridCircularBuffer<Integer> buf = new GridCircularBuffer<>(size);
        final AtomicInteger itemGen = new AtomicInteger();

        info("Created buffer: " + buf);

        final int iterCnt = 1_000_000;

        multithreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                for (int i = 0; i < iterCnt; i++) {
                    int item = itemGen.getAndIncrement();

                    buf.add(item);
                }

                return null;
            }
        }, 32);

        info("Buffer: " + buf);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMutliThreaded2() throws Exception {
        int size = 256 * 1024;

        final GridCircularBuffer<Integer> buf = new GridCircularBuffer<>(size);
        final AtomicInteger itemGen = new AtomicInteger();

        info("Created buffer: " + buf);

        final int iterCnt = 10_000;
        final ConcurrentLinkedDeque8<Integer> evictedQ = new ConcurrentLinkedDeque8<>();
        final ConcurrentLinkedDeque8<Integer> putQ = new ConcurrentLinkedDeque8<>();

        multithreaded(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    for (int i = 0; i < iterCnt; i++) {
                        int item = itemGen.getAndIncrement();

                        putQ.add(item);

                        Integer evicted = buf.add(item);

                        if (evicted != null)
                            evictedQ.add(evicted);
                    }

                    return null;
                }
            },
            8);

        evictedQ.addAll(buf.items());

        assert putQ.containsAll(evictedQ);
        assert evictedQ.sizex() == putQ.sizex();

        info("Buffer: " + buf);
    }
}
