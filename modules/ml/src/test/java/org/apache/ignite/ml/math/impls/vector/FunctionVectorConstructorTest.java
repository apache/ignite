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
import java.util.function.IntToDoubleFunction;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IntDoubleToVoidFunction;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** */
public class FunctionVectorConstructorTest {
    /** */
    private static final int IMPOSSIBLE_SIZE = -1;

    /** */
    @Test(expected = UnsupportedOperationException.class)
    public void mapInvalidArgsTest() {
        assertEquals("Expect exception due to invalid args.", IMPOSSIBLE_SIZE,
            new FunctionVector(new HashMap<String, Object>() {{
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
            -1, new FunctionVector(test).size());
    }

    /** */
    @Test(expected = ClassCastException.class)
    public void mapInvalidParamTypeTest() {
        final Map<String, Object> test = new HashMap<String, Object>() {{
            put("size", "whatever");

            put("getFunc", (IntToDoubleFunction)i -> i);
        }};

        assertEquals("Expect exception due to invalid param type.", IMPOSSIBLE_SIZE,
            new FunctionVector(test).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void mapNullTest() {
        //noinspection ConstantConditions
        assertEquals("Null map args.", IMPOSSIBLE_SIZE,
            new FunctionVector(null).size());
    }

    /** */
    @Test
    public void mapTest() {
        assertEquals("Size from args.", 99,
            new FunctionVector(new HashMap<String, Object>() {{
                put("size", 99);

                put("getFunc", (IgniteFunction<Integer, Double>)i -> (double)i);
            }}).size());

        assertEquals("Size from args with setFunc.", 99,
            new FunctionVector(new HashMap<String, Object>() {{
                put("size", 99);

                put("getFunc", (IgniteFunction<Integer, Double>)i -> (double)i);

                put("setFunc", (IntDoubleToVoidFunction)(integer, aDouble) -> {
                });
            }}).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void negativeSizeTest() {
        assertEquals("Negative size.", IMPOSSIBLE_SIZE,
            new FunctionVector(-1, (i) -> (double)i).size());
    }

    /** */
    @Test(expected = AssertionError.class)
    public void zeroSizeTest() {
        assertEquals("0 size.", IMPOSSIBLE_SIZE,
            new FunctionVector(0, (i) -> (double)i).size());
    }

    /** */
    @Test
    public void primitiveTest() {
        assertEquals("1 size.", 1,
            new FunctionVector(1, (i) -> (double)i).size());

        assertEquals("2 size.", 2,
            new FunctionVector(2, (i) -> (double)i).size());
    }
}
