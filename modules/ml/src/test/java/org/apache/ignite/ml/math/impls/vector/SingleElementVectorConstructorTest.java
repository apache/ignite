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

package org.apache.ignite.ml.math.impls.vector;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** */
public class SingleElementVectorConstructorTest {
    /** */
    private static final int IMPOSSIBLE_SIZE = -1;

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void mapInvalidArgsTest() {
        assertEquals("Expect exception due to invalid args.", IMPOSSIBLE_SIZE,
            new SingleElementVector(new HashMap<String, Object>() {{
                put("invalid", 99);
            }}).size());
    }

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void mapMissingArgsTest() {
        final Map<String, Object> test = new HashMap<String, Object>() {{
            put("size", 1);

            put("paramMissing", "whatever");
        }};

        assertEquals("Expect exception due to missing args.",
            -1, new SingleElementVector(test).size());
    }

    /** */
    @Test(expected = ClassCastException.class)
    public void mapInvalidParamTypeTest() {
        final Map<String, Object> test = new HashMap<String, Object>() {{
            put("size", "whatever");

            put("index", 0);
            put("value", 1.0);
        }};

        assertEquals("Expect exception due to invalid param type.", IMPOSSIBLE_SIZE,
            new SingleElementVector(test).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void mapNullTest() {
        //noinspection ConstantConditions
        assertEquals("Null map args.", IMPOSSIBLE_SIZE,
            new SingleElementVector(null).size());
    }

    /** */
    @Test
    public void mapTest() {
        assertEquals("Size from array in args.", 99,
            new SingleElementVector(new HashMap<String, Object>() {{
                put("size", 99);
                put("index", 0);
                put("value", 1.0);
            }}).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void negativeSizeTest() {
        assertEquals("Negative size.", IMPOSSIBLE_SIZE,
            new SingleElementVector(-1, 0, 1.0).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void zeroSizeTest() {
        assertEquals("Zero size.", IMPOSSIBLE_SIZE,
            new SingleElementVector(0, 0, 1.0).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void wrongIndexTest() {
        //noinspection ConstantConditions
        assertEquals("Wrong index.", IMPOSSIBLE_SIZE,
            new SingleElementVector(1, 2, 1.0).size());
    }

    /** */
    @Test
    public void basicTest() {
        final int[] sizes = new int[] {1, 4, 8};

        for (int size : sizes)
            for (int idx = 0; idx < size; idx++)
                basicTest(size, idx);
    }

    /** */
    private void basicTest(int size, int idx) {
        final Double expVal = (double)(size - idx);

        Vector v = new SingleElementVector(size, idx, expVal);

        assertTrue("Expect value " + expVal + " at index " + idx + " for size " + size,
            expVal.equals(v.get(idx)));

        final double delta = 1.0;

        v.set(idx, expVal - delta);

        assertTrue("Expect value " + expVal + " at index " + idx + " for size " + size,
            expVal.equals(v.get(idx) + delta));

        final Double zero = 0.0;

        for (int i = 0; i < size; i++) {
            if (i == idx)
                continue;

            assertTrue("Expect zero at index " + i + " for size " + size,
                zero.equals(v.get(i)));

            boolean eCaught = false;

            try {
                v.set(i, 1.0);
            }
            catch (UnsupportedOperationException uoe) {
                eCaught = true;
            }

            assertTrue("Expect " + java.lang.UnsupportedOperationException.class.getSimpleName()
                + " at index " + i + " for size " + size, eCaught);
        }
    }
}
