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

package org.apache.ignite.lang;

import java.util.ArrayList;
import java.util.Collection;
import org.apache.ignite.internal.util.lang.GridIterator;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

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