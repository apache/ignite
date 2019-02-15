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

package org.apache.ignite.testframework.test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.testframework.configvariations.VariationsIterator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test start iterator.
 */
public class VariationsIteratorTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void test1() throws Exception {
        Object[][] arr = new Object[][] {
            {0, 1},
            {0, 1},
            {0, 1},
        };

        checkIterator(arr);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test2() throws Exception {
        Object[][] arr = new Object[][] {
            {0},
            {0, 1, 2},
            {0, 1},
            {0, 1, 2, 3, 4, 5},
        };

        checkIterator(arr);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test3() throws Exception {
        Object[][] arr = new Object[][] {
            {0, 1, 2, 3, 4, 5},
            {0, 1, 2},
            {0, 1},
            {0},
        };

        checkIterator(arr);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void test4() throws Exception {
        Object[][] arr = new Object[][]{
            {0,1,2},
            {0,1},
            {0,1,2,4},
            {0,1},
            {0},
            {0},
            {0,1,2,4},
        };

        checkIterator(arr);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimple() throws Exception {
        Object[][] arr = new Object[][] {
            {0},
        };

        checkIterator(arr);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testSimple2() throws Exception {
        Object[][] arr = new Object[][] {
            {0},
            {0},
        };

        checkIterator(arr);
    }

    /**
     * @param arr Array.
     */
    private void checkIterator(Object[][] arr) {
        int expSize = 1;
        int significantParamsCnt = 1;

        for (int i = 0; i < arr.length; i++) {
            Object[] objects = arr[i];

            System.out.println(">>> " + i + ": " + objects.length);

            expSize *= objects.length;

            if (objects.length > 1)
                significantParamsCnt++;
        }

        System.out.println("Iteration info [expSize=" + expSize + ", significantParamsCnt=" + significantParamsCnt + "]");

        Set<int[]> states = new HashSet<>();

        int step = 0;

        for (VariationsIterator it = new VariationsIterator(arr); it.hasNext(); ) {
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

        assertEquals(expSize, states.size());
    }
}
