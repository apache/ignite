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

package org.apache.ignite.math.impls.vector;

import org.apache.ignite.math.StorageConstants;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/** */
public class SparseLocalVectorConstructorTest {
    /** */ private static final int IMPOSSIBLE_SIZE = -1;

    /** */ @Test(expected = org.apache.ignite.math.UnsupportedOperationException.class)
    public void mapInvalidArgsTest() {
        assertEquals("Expect exception due to invalid args.", IMPOSSIBLE_SIZE,
            new SparseLocalVector(new HashMap<String, Object>(){{put("invalid", 99);}}).size());
    }

    /** */ @Test(expected = org.apache.ignite.math.UnsupportedOperationException.class)
    public void mapMissingArgsTest() {
        final Map<String, Object> test = new HashMap<String, Object>(){{
            put("arr",  new double[0]);

            put("shallowCopyMissing", "whatever");
        }};

        assertEquals("Expect exception due to missing args.", IMPOSSIBLE_SIZE,
            new SparseLocalVector(test).size());
    }

    /** */ @Test(expected = org.apache.ignite.math.UnsupportedOperationException.class)
    public void mapInvalidArrTypeTest() {
        final Map<String, Object> test = new HashMap<String, Object>(){{
            put("arr", new int[0]);

            put("shallowCopy", true);
        }};

        assertEquals("Expect exception due to invalid arr type.", IMPOSSIBLE_SIZE,
            new SparseLocalVector(test).size());
    }

    /** */ @Test(expected = org.apache.ignite.math.UnsupportedOperationException.class)
    public void mapInvalidCopyTypeTest() {
        final Map<String, Object> test = new HashMap<String, Object>(){{
            put("arr", new double[0]);

            put("shallowCopy", 0);
        }};

        assertEquals("Expect exception due to invalid copy type.", IMPOSSIBLE_SIZE,
            new SparseLocalVector(test).size());
    }

    /** */ @Test(expected = AssertionError.class)
    public void mapNullTest() {
        //noinspection ConstantConditions
        assertEquals("Null map args.", IMPOSSIBLE_SIZE,
            new SparseLocalVector(null).size());
    }

    /** */ @Test
    public void mapTest() {
        assertEquals("Size from args, random access.", 99,
            new SparseLocalVector(new HashMap<String, Object>(){{
                put("size", 99);
                put("acsMode", StorageConstants.RANDOM_ACCESS_MODE);
            }}).size());

        assertEquals("Size from args, sequential access.", 99,
            new SparseLocalVector(new HashMap<String, Object>(){{
                put("size", 99);
                put("acsMode", StorageConstants.SEQUENTIAL_ACCESS_MODE);
            }}).size());
    }

    /** */ @Test(expected = AssertionError.class)
    public void negativeSizeTest() {
        assertEquals("Negative size.", IMPOSSIBLE_SIZE,
            new SparseLocalVector(-1, 1).size());
    }

    /** */ @Test(expected = AssertionError.class)
    public void zeroSizeTest() {
        assertEquals("0 size.", IMPOSSIBLE_SIZE,
            new SparseLocalVector(0, 1).size());
    }

    /** */ @Test
    public void primitiveTest() {
        assertEquals("1 size, random access.", 1,
            new SparseLocalVector(1, StorageConstants.RANDOM_ACCESS_MODE).size());

        assertEquals("1 size, sequential access.", 1,
            new SparseLocalVector(1, StorageConstants.SEQUENTIAL_ACCESS_MODE).size());

    }
}
