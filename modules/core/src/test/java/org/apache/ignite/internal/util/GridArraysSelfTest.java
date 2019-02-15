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

package org.apache.ignite.internal.util;

import java.util.Arrays;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.internal.util.GridArrays.clearTail;
import static org.apache.ignite.internal.util.GridArrays.remove;
import static org.apache.ignite.internal.util.GridArrays.set;

/**
 */
@RunWith(JUnit4.class)
public class GridArraysSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String[] EMPTY = {};

    /**
     */
    @Test
    public void testSet() {
        String[] arr = set(EMPTY, 4, "aa");

        assertNotSame(EMPTY, arr);
        assertEquals("aa", arr[4]);

        for (int i = 0; i < arr.length; i++) {
            if (i != 4)
                assertNull(arr[i]);
        }

        String[] oldArr = arr;

        arr = set(arr, 1, "bb");

        assertSame(oldArr, arr);
        assertEquals("aa", arr[4]);
        assertEquals("bb", arr[1]);

        for (int i = 0; i < arr.length; i++) {
            if (i != 1 && i != 4)
                assertNull(arr[i]);
        }

        arr = set(arr, 100, "cc");

        assertNotSame(oldArr, arr);
        assertEquals("aa", arr[4]);
        assertEquals("bb", arr[1]);
        assertEquals("cc", arr[100]);

        for (int i = 0; i < arr.length; i++) {
            if (i != 1 && i != 4 && i != 100)
                assertNull(arr[i]);
        }
    }

    /**
     */
    @Test
    public void testClearTail() {
        String[] arr = new String[10];

        Arrays.fill(arr, "zz");

        clearTail(arr, 11);

        for (String s : arr)
            assertEquals("zz", s);

        clearTail(arr, 10);

        for (String s : arr)
            assertEquals("zz", s);

        clearTail(arr, 9);

        assertNull(arr[9]);

        for (int i = 0; i < 9 ; i++)
            assertEquals("zz", arr[i]);

        clearTail(arr, 7);

        assertNull(arr[7]);
        assertNull(arr[8]);
        assertNull(arr[9]);

        for (int i = 0; i < 7 ; i++)
            assertEquals("zz", arr[i]);
    }

    /**
     */
    @Test
    public void testRemoveLong() {
        long[] arr = {0,1,2,3,4,5,6};

        assertTrue(Arrays.equals(new long[]{1,2,3,4,5,6}, remove(arr, 0)));
        assertTrue(Arrays.equals(new long[]{0,2,3,4,5,6}, remove(arr, 1)));
        assertTrue(Arrays.equals(new long[]{0,1,2,3,5,6}, remove(arr, 4)));
        assertTrue(Arrays.equals(new long[]{0,1,2,3,4,5}, remove(arr, 6)));
        assertTrue(Arrays.equals(new long[0], remove(new long[]{1}, 0)));
    }

    /**
     */
    @Test
    public void testRemove() {
        Integer[] arr = {0,1,2,3,4,5,6};

        assertTrue(Arrays.equals(new Integer[]{1,2,3,4,5,6}, remove(arr, 0)));
        assertTrue(Arrays.equals(new Integer[]{0,2,3,4,5,6}, remove(arr, 1)));
        assertTrue(Arrays.equals(new Integer[]{0,1,2,3,5,6}, remove(arr, 4)));
        assertTrue(Arrays.equals(new Integer[]{0,1,2,3,4,5}, remove(arr, 6)));
        assertTrue(Arrays.equals(new Integer[0], remove(new Integer[]{1}, 0)));
    }
}
