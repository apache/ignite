/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

import org.gridgain.grid.util.typedef.internal.*;
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
