/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.lang;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * GridFunc performance test.
 */
@GridCommonTest(group = "Lang")
@RunWith(JUnit4.class)
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
    @Test
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

        IgnitePredicate<Integer> p1 = new IgnitePredicate<Integer>() {
            @Override public boolean apply(Integer e) {
                return e % 2 == 0;
            }
        };
        IgnitePredicate<Integer> p2 = new IgnitePredicate<Integer>() {
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
