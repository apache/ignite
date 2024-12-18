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

package org.apache.ignite.internal.binary;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.internal.util.tostring.GridToStringBuilder.DFLT_TO_STRING_COLLECTION_LIMIT;

/**
 * Tests for {@code BinaryObject.toString()}.
 */
public class BinaryObjectToStringSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testToString() throws Exception {
        MyObject obj = new MyObject();

        obj.arr = new Object[] {111, "aaa", obj};
        obj.col = Arrays.asList(222, "bbb", obj);

        obj.map = new HashMap();

        obj.map.put(10, 333);
        obj.map.put(20, "ccc");
        obj.map.put(30, obj);

        BinaryObject bo = grid().binary().toBinary(obj);

        // Check that toString() doesn't fail with StackOverflowError or other exceptions.
        bo.toString();
    }

    /**
     * Check if toString produce limited representation respecting the
     * {@link IgniteSystemProperties#IGNITE_TO_STRING_COLLECTION_LIMIT } limit.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testToStringForLargeArrays() throws Exception {
        List<String> types = Arrays.asList(
            "byte",
            "short",
            "int",
            "long",
            "float",
            "double",
            "char",
            "boolean",
            "BigDecimal",
            "Object",
            "Iterable",
            "Map"
        );

        for (String type : types) {
            assertFalse(String.format("type=%s, size=%d", type, DFLT_TO_STRING_COLLECTION_LIMIT - 1),
                    containElipsis(type, getObject(type, DFLT_TO_STRING_COLLECTION_LIMIT - 1)));

            assertFalse(String.format("type=%s, size=%d", type, DFLT_TO_STRING_COLLECTION_LIMIT),
                    containElipsis(type, getObject(type, DFLT_TO_STRING_COLLECTION_LIMIT)));

            assertTrue(String.format("type=%s, size=%d", type, DFLT_TO_STRING_COLLECTION_LIMIT + 1),
                    containElipsis(type, getObject(type, DFLT_TO_STRING_COLLECTION_LIMIT + 1)));
        }
    }

    /** */
    private Object getObject(String type, int size) {
        switch (type) {
            case "byte":
                return new byte[size];
            case "short":
                return new short[size];
            case "int":
                return new int[size];
            case "long":
                return new long[size];
            case "float":
                return new float[size];
            case "double":
                return new double[size];
            case "char":
                return new char[size];
            case "boolean":
                return new boolean[size];
            case "BigDecimal":
                return new BigDecimal[size];
            case "Object":
                return new MyObject[size];
            case "Map":
                Map<Integer, MyObject> map = new HashMap<>();

                for (int i = 0; i < size; i++)
                    map.put(i, new MyObject());

                return map;
            default:
                // Iterable
                return Arrays.asList(new String[size]);
        }
    }

    /** */
    private boolean containElipsis(String type, Object val) {
        BinaryObject bo = grid().binary()
                .builder(type)
                .setField("field", val)
                .build();

        return bo.toString().contains("...");
    }

    /**
     */
    private static class MyObject {
        /** Object array. */
        private Object[] arr;

        /** Collection. */
        private Collection col;

        /** Map. */
        private Map map;
    }
}
