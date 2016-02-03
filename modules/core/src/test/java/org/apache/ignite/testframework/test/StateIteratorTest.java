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

package org.apache.ignite.testframework.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import junit.framework.TestCase;
import org.apache.ignite.testframework.config.generator.StateIterator;

/**
 * Test start iterator.
 */
public class StateIteratorTest extends TestCase {
    /**
     * @throws Exception If failed.
     */
    public void test1() throws Exception {
        Object[][] arr = new Object[][] {
            {0, 1},
            {0, 1},
            {0, 1},
        };

        checkIterator(arr, 2 * 2 * 2);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("PointlessArithmeticExpression")
    public void test2() throws Exception {
        Object[][] arr = new Object[][] {
            {0},
            {0, 1, 2},
            {0, 1},
            {0, 1, 2, 3, 4, 5},
        };

        checkIterator(arr, 1 * 3 * 2 * 6);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("PointlessArithmeticExpression")
    public void test3() throws Exception {
        Object[][] arr = new Object[][] {
            {0, 1, 2, 3, 4, 5},
            {0, 1, 2},
            {0, 1},
            {0},
        };

        checkIterator(arr, 6 * 3 * 2 * 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimple() throws Exception {
        Object[][] arr = new Object[][] {
            {0},
        };

        checkIterator(arr, 1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testSimple2() throws Exception {
        Object[][] arr = new Object[][] {
            {0},
            {0},
        };

        checkIterator(arr, 1);
    }

    /**
     * @param arr Array.
     * @param expStatesNum Number.
     */
    private void checkIterator(Object[][] arr, int expStatesNum) {
        Set<int[]> states = new HashSet<>();

        int step = 0;

        for (StateIterator it = new StateIterator(arr); it.hasNext(); ) {
            int[] state = it.next();

            System.out.println(Arrays.toString(state));

            for (int[] state2 : states) {
                if (Arrays.equals(state, state2))
                    fail("Got equal states on step " + step + " [state=" + Arrays.toString(state)
                        + ", state2=" + Arrays.toString(state2));
            }

            states.add(state);

            step++;
        }

        assertEquals(expStatesNum, states.size());
    }
}
