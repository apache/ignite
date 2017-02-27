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
public class SequentialAccessSparseLocalOnHeapVectorConstructorTest {
    /** */ private static final int IMPOSSIBLE_SIZE = -1;

    /** */ @Test(expected = org.apache.ignite.math.UnsupportedOperationException.class)
    public void mapInvalidArgsTest() {
        assertEquals("Expect exception due to invalid args.", IMPOSSIBLE_SIZE,
            new SequentialAccessSparseLocalOnHeapVector(new HashMap<String, Object>(){{put("invalid", 99);}}).size());
    }

    /** */ @Test(expected = org.apache.ignite.math.UnsupportedOperationException.class)
    public void mapMissingArgsTest() {
        final Map<String, Object> test = new HashMap<String, Object>(){{
            put("arr",  new double[0]);

            put("shallowCopyMissing", "whatever");
        }};

        assertEquals("Expect exception due to missing args.",
            -1, new SequentialAccessSparseLocalOnHeapVector(test).size());
    }

    /** */ @Test(expected = org.apache.ignite.math.UnsupportedOperationException.class)
    public void mapInvalidArrTypeTest() {
        final Map<String, Object> test = new HashMap<String, Object>(){{
            put("arr", new int[0]);

            put("shallowCopy", true);
        }};

        assertEquals("Expect exception due to invalid arr type.", IMPOSSIBLE_SIZE,
            new SequentialAccessSparseLocalOnHeapVector(test).size());
    }

    /** */ @Test(expected = org.apache.ignite.math.UnsupportedOperationException.class)
    public void mapInvalidCopyTypeTest() {
        final Map<String, Object> test = new HashMap<String, Object>(){{
            put("arr", new double[0]);

            put("shallowCopy", 0);
        }};

        assertEquals("Expect exception due to invalid copy type.", IMPOSSIBLE_SIZE,
            new SequentialAccessSparseLocalOnHeapVector(test).size());
    }

    /** */ @Test(expected = AssertionError.class)
    public void mapNullTest() {
        assertEquals("Null map args.", IMPOSSIBLE_SIZE,
            new SequentialAccessSparseLocalOnHeapVector((Map<String, Object>)null).size());
    }

    /** */ @Test
    public void mapTest() {
        assertEquals("Size from args.", 0,
            new SequentialAccessSparseLocalOnHeapVector(new HashMap<String, Object>(){{ put("size", 99); }}).size());

        final double[] test = new double[99];

        assertEquals("Size from array in args.", test.length,
            new SequentialAccessSparseLocalOnHeapVector(new HashMap<String, Object>(){{
                put("arr", test);
                put("copy", false);
            }}).size());

        assertEquals("Size from array in args, shallow copy.", test.length,
            new SequentialAccessSparseLocalOnHeapVector(new HashMap<String, Object>(){{
                put("arr", test);
                put("copy", true);
            }}).size());
    }

    /** */ @Test(expected = AssertionError.class)
    public void negativeSizeTest() {
        assertEquals("Negative size.", IMPOSSIBLE_SIZE,
            new SequentialAccessSparseLocalOnHeapVector(-1).size());
    }

    /** */ @Test
    public void primitiveTest() {
        assertEquals("0 size.", 0,
            new SequentialAccessSparseLocalOnHeapVector(0).size());

        assertEquals("1 size.", 0,
            new SequentialAccessSparseLocalOnHeapVector(1).size());
    }
}
