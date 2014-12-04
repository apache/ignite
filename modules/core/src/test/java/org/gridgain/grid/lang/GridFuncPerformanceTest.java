/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.lang;

import org.apache.ignite.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.testframework.junits.common.*;
import java.util.*;

/**
 * GridFunc performance test.
 */
@GridCommonTest(group = "Lang")
public class GridFuncPerformanceTest extends GridCommonAbstractTest {
    /**
     *  Creates test.
     */
    public GridFuncPerformanceTest() {
        super(/*start grid*/false);
    }

    /**
     *
     */
    public void testTransformingIteratorPerformance() {
        // Warmup.
        testBody();
        testBody();
        testBody();

        long r1 = testBody();
        long r2 = testBody();
        long r3 = testBody();

        double r = (r1 + r2 + r3) / 3.f;

        System.out.println("Average result is: " + Math.round(r) + "msec.");
    }

    /**
     *
     * @return Duration of the test.
     */
    @SuppressWarnings({"UnusedDeclaration"})
    private long testBody() {
        int MAX = 20000000;

        Collection<Integer> l = new ArrayList<>(MAX);

        for (int i = 0; i < MAX / 10; i++)
            l.add(i);

        IgniteClosure<Integer, Integer> c = new IgniteClosure<Integer, Integer>() {
            @Override public Integer apply(Integer e) {
                return e;
            }
        };

        GridPredicate<Integer> p1 = new GridPredicate<Integer>() {
            @Override public boolean apply(Integer e) {
                return e % 2 == 0;
            }
        };
        GridPredicate<Integer> p2 = new GridPredicate<Integer>() {
            @Override public boolean apply(Integer e) {
                return e % 2 != 0;
            }
        };

        GridIterator<Integer> iter = F.iterator(l, c, true, p1, p2);

        long n = 0;

        long start = System.currentTimeMillis();

        for (Integer i : iter)
            n += i;

        long duration = System.currentTimeMillis() - start;

        System.out.println("Duration: " + duration + "msec.");

        return duration;
    }
}
