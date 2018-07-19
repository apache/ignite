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

package org.apache.ignite.ml.math.primitives.vector;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** */
public class DenseVectorConstructorTest {
    /** */
    private static final int IMPOSSIBLE_SIZE = -1;

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void mapInvalidArgsTest() {
        assertEquals("Expect exception due to invalid args.", IMPOSSIBLE_SIZE,
            new DenseVector(new HashMap<String, Object>() {{
                put("invalid", 99);
            }}).size());
    }

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void mapMissingArgsTest() {
        final Map<String, Object> test = new HashMap<String, Object>() {{
            put("arr", new double[0]);
            put("shallowCopyMissing", "whatever");
        }};

        assertEquals("Expect exception due to missing args.", IMPOSSIBLE_SIZE,
            new DenseVector(test).size());
    }

    /** */
    @Test(expected = ClassCastException.class)
    public void mapInvalidArrTypeTest() {
        final Map<String, Object> test = new HashMap<String, Object>() {{
            put("size", "whatever");
        }};

        assertEquals("Expect exception due to invalid arr type.", IMPOSSIBLE_SIZE,
            new DenseVector(test).size());
    }

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void mapInvalidCopyTypeTest() {
        final Map<String, Object> test = new HashMap<String, Object>() {{
            put("arr", new double[0]);
            put("shallowCopy", 0);
        }};

        assertEquals("Expect exception due to invalid copy type.", IMPOSSIBLE_SIZE,
            new DenseVector(test).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void mapNullTest() {
        //noinspection ConstantConditions
        assertEquals("Null map args.", IMPOSSIBLE_SIZE,
            new DenseVector((Map<String, Object>)null).size());
    }

    /** */
    @Test
    public void mapTest() {
        assertEquals("Size from args.", 99,
            new DenseVector(new HashMap<String, Object>() {{
                put("size", 99);
            }}).size());

        final double[] test = new double[99];

        assertEquals("Size from array in args.", test.length,
            new DenseVector(new HashMap<String, Object>() {{
                put("arr", test);
                put("copy", false);
            }}).size());

        assertEquals("Size from array in args, shallow copy.", test.length,
            new DenseVector(new HashMap<String, Object>() {{
                put("arr", test);
                put("copy", true);
            }}).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void negativeSizeTest() {
        assertEquals("Negative size.", IMPOSSIBLE_SIZE,
            new DenseVector(-1).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void nullCopyTest() {
        assertEquals("Null array to non-shallow copy.", IMPOSSIBLE_SIZE,
            new DenseVector(null, false).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void nullDefaultCopyTest() {
        assertEquals("Null array default copy.", IMPOSSIBLE_SIZE,
            new DenseVector((double[])null).size());
    }

    /** */
    @Test(expected = NullPointerException.class)
    public void defaultConstructorTest() {
        assertEquals("Default constructor.", IMPOSSIBLE_SIZE,
            new DenseVector().size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void nullArrShallowCopyTest() {
        assertEquals("Null array shallow copy.", IMPOSSIBLE_SIZE,
            new DenseVector(null, true).size());
    }

    /** */
    @Test
    public void primitiveTest() {
        assertEquals("0 size shallow copy.", 0,
            new DenseVector(new double[0], true).size());

        assertEquals("0 size.", 0,
            new DenseVector(new double[0], false).size());

        assertEquals("1 size shallow copy.", 1,
            new DenseVector(new double[1], true).size());

        assertEquals("1 size.", 1,
            new DenseVector(new double[1], false).size());

        assertEquals("0 size default copy.", 0,
            new DenseVector(new double[0]).size());

        assertEquals("1 size default copy.", 1,
            new DenseVector(new double[1]).size());
    }
}
