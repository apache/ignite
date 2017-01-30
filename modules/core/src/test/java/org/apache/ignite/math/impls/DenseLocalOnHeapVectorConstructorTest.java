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

package org.apache.ignite.math.impls;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** */
public class DenseLocalOnHeapVectorConstructorTest {
    /** */ private static final int IMPOSSIBLE_SIZE = -1;

    /** */ @Test(expected = org.apache.ignite.math.UnsupportedOperationException.class)
    public void mapInvalidArgsTest() {
        assertEquals("expect exception due to invalid args",IMPOSSIBLE_SIZE,
            new DenseLocalOnHeapVector(new HashMap<String, Object>(){{put("invalid", 99);}}).size());
    }

    /** */ @Test(expected = org.apache.ignite.math.UnsupportedOperationException.class)
    public void mapMissingArgsTest() {
        final Map<String, Object> test = new HashMap<String, Object>(){{
            put("arr",  new double[0]);

            put("shallowCopyMissing", "whatever");
        }};

        assertEquals("expect exception due to missing args",
            -1, new DenseLocalOnHeapVector(test).size());
    }

    /** */ @Test(expected = ClassCastException.class)
    public void mapInvalidArrTypeTest() {
        final Map<String, Object> test = new HashMap<String, Object>(){{
            put("arr", new int[0]);

            put("shallowCopy", true);
        }};

        assertEquals("expect exception due to invalid arr type", IMPOSSIBLE_SIZE,
            new DenseLocalOnHeapVector(test).size());
    }

    /** */ @Test(expected = ClassCastException.class)
    public void mapInvalidCopyTypeTest() {
        final Map<String, Object> test = new HashMap<String, Object>(){{
            put("arr", new double[0]);

            put("shallowCopy", 0);
        }};

        assertEquals("expect exception due to invalid copy type", IMPOSSIBLE_SIZE,
            new DenseLocalOnHeapVector(test).size());
    }

    /** */ @Test
    public void mapTest() {
        assertEquals("default size for null args",100,
            new DenseLocalOnHeapVector((Map<String, Object>)null).size());

        assertEquals("size from args", 99,
            new DenseLocalOnHeapVector(new HashMap<String, Object>(){{ put("size", 99); }}).size());

        final double[] test = new double[99];

        assertEquals("size from array in args", test.length,
            new DenseLocalOnHeapVector(new HashMap<String, Object>(){{
                put("arr", test);
                put("shallowCopy", false);
            }}).size());

        assertEquals("size from array in args, shallow copy", test.length,
            new DenseLocalOnHeapVector(new HashMap<String, Object>(){{
                put("arr", test);
                put("shallowCopy", true);
            }}).size());
    }

    /** */ @Test(expected = NegativeArraySizeException.class)
    public void negativeSizeTest() {
        assertEquals("negative size", IMPOSSIBLE_SIZE,
            new DenseLocalOnHeapVector(-1).size());
    }

    /** */ @Test(expected = NullPointerException.class)
    public void nullCopyTest() {
        assertEquals("null array to non-shallow copy", IMPOSSIBLE_SIZE,
            new DenseLocalOnHeapVector(null, false).size());
    }

    /** */ @Test(expected = NullPointerException.class)
    public void nullDefaultCopyTest() {
        assertEquals("null array default copy", IMPOSSIBLE_SIZE,
            new DenseLocalOnHeapVector((double[])null).size());
    }

    /** */ @Test
    public void primitiveTest() {
        assertEquals("default constructor", 100,
            new DenseLocalOnHeapVector().size());

        assertEquals("null array shallow copy", 0,
            new DenseLocalOnHeapVector(null, true).size());

        assertEquals("0 size shallow copy", 0,
            new DenseLocalOnHeapVector(new double[0], true).size());

        assertEquals("0 size", 0,
            new DenseLocalOnHeapVector(new double[0], false).size());

        assertEquals("1 size shallow copy", 1,
            new DenseLocalOnHeapVector(new double[1], true).size());

        assertEquals("1 size", 1,
            new DenseLocalOnHeapVector(new double[1], false).size());

        assertEquals("0 size default copy", 0,
            new DenseLocalOnHeapVector(new double[0]).size());

        assertEquals("1 size default copy", 1,
            new DenseLocalOnHeapVector(new double[1]).size());
    }
}
