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

package org.apache.ignite.internal.configuration.tree;

import static org.apache.ignite.internal.configuration.util.ConfigurationUtil.leafNodeVisitor;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.internal.configuration.asm.ConfigurationAsmGenerator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Test configuration with array of primitives and {@code String} fields.
 */
public class ConfigurationArrayTest {
    /**
     *
     */
    @Config
    public static class TestArrayConfigurationSchema {
        /**
         *
         */
        @Value
        public boolean[] booleanArray;

        /**
         *
         */
        @Value
        public byte[] byteArray;

        /**
         *
         */
        @Value
        public short[] shortArray;

        /**
         *
         */
        @Value
        public int[] intArray;

        /**
         *
         */
        @Value
        public long[] longArray;

        /**
         *
         */
        @Value
        public char[] charArray;

        /**
         *
         */
        @Value
        public float[] floatArray;

        /**
         *
         */
        @Value
        public double[] doubleArray;

        /**
         *
         */
        @Value
        public String[] stringArray;
    }

    /**
     *
     */
    private static ConfigurationAsmGenerator cgen = new ConfigurationAsmGenerator();

    /**
     *
     */
    @BeforeAll
    public static void beforeAll() {
        cgen.compileRootSchema(TestArrayConfigurationSchema.class, Map.of(), Map.of());
    }

    /**
     *
     */
    @AfterAll
    public static void afterAll() {
        cgen = null;
    }

    /**
     * Parameterized test source with all supported array types.
     */
    static Stream<Class<?>> supportedTypes() {
        return Stream.of(
                boolean[].class,
                byte[].class,
                short[].class,
                int[].class,
                long[].class,
                char[].class,
                float[].class,
                double[].class,
                String[].class
        );
    }

    /**
     * Test array node change operation.
     */
    @ParameterizedTest
    @MethodSource("supportedTypes")
    public void testChange(Class<?> type) throws Exception {
        InnerNode arrayNode = cgen.instantiateNode(TestArrayConfigurationSchema.class);

        Object initialValue = createTestValue(type);

        changeArray(arrayNode, initialValue);

        // test that init method set values successfully
        assertThat(getArray(arrayNode, type), is(initialValue));
        assertThat(getViewField(arrayNode, type), is(initialValue));

        // test that field is not the same as initialValue
        assertThat(getArray(arrayNode, type), not(sameInstance(initialValue)));

        // test that returned array is a copy of the field
        assertThat(getArray(arrayNode, type), not(sameInstance(getViewField(arrayNode, type))));

        Object newValue = createTestValue(type);

        changeArray(arrayNode, newValue);

        // test that change method set values successfully
        assertThat(getArray(arrayNode, type), is(newValue));
        assertThat(getViewField(arrayNode, type), is(newValue));
    }

    /**
     * Gets an array field from the given {@code InnerNode}.
     */
    private static <T> T getArray(InnerNode arrayNode, Class<T> cls) {
        return cls.cast(arrayNode.traverseChild(getFieldName(cls), leafNodeVisitor(), true));
    }

    /**
     * Calls one of the {@link TestArrayView} getters based on the field type.
     */
    private static <T> T getViewField(InnerNode arrayNode, Class<T> cls) throws Exception {
        String methodName = getFieldName(cls);

        return cls.cast(arrayNode.getClass().getMethod(methodName).invoke(arrayNode));
    }

    /**
     * Calls one of the {@link TestArrayChange} setters based on the value type.
     */
    private static void changeArray(InnerNode arrayNode, Object newValue) throws Exception {
        String fieldName = getFieldName(newValue.getClass());

        String methodName = "change" + Character.toUpperCase(fieldName.charAt(0)) + fieldName.substring(1);

        arrayNode.getClass().getMethod(methodName, newValue.getClass()).invoke(arrayNode, newValue);
    }

    /**
     * Returns the field name of {@link TestArrayConfigurationSchema} based on its type.
     */
    private static String getFieldName(Class<?> cls) {
        if (cls == boolean[].class) {
            return "booleanArray";
        } else if (cls == byte[].class) {
            return "byteArray";
        } else if (cls == short[].class) {
            return "shortArray";
        } else if (cls == int[].class) {
            return "intArray";
        } else if (cls == long[].class) {
            return "longArray";
        } else if (cls == char[].class) {
            return "charArray";
        } else if (cls == float[].class) {
            return "floatArray";
        } else if (cls == double[].class) {
            return "doubleArray";
        } else if (cls == String[].class) {
            return "stringArray";
        } else {
            throw new AssertionError("Invalid field type: " + cls);
        }
    }

    /**
     * Creates a randomized test array value of the given type.
     */
    private static Object createTestValue(Class<?> cls) {
        var random = new Random();

        if (cls == boolean[].class) {
            return new boolean[]{random.nextBoolean(), random.nextBoolean(), random.nextBoolean()};
        } else if (cls == byte[].class) {
            return new byte[]{(byte) random.nextInt(), (byte) random.nextInt(), (byte) random.nextInt()};
        } else if (cls == short[].class) {
            return new short[]{(short) random.nextInt(), (short) random.nextInt(), (short) random.nextInt()};
        } else if (cls == int[].class) {
            return new int[]{random.nextInt(), random.nextInt(), random.nextInt()};
        } else if (cls == long[].class) {
            return new long[]{random.nextLong(), random.nextLong(), random.nextLong()};
        } else if (cls == char[].class) {
            return new char[]{(char) random.nextInt(), (char) random.nextInt(), (char) random.nextInt()};
        } else if (cls == float[].class) {
            return new float[]{random.nextFloat(), random.nextFloat(), random.nextFloat()};
        } else if (cls == double[].class) {
            return new double[]{random.nextDouble(), random.nextDouble(), random.nextDouble()};
        } else if (cls == String[].class) {
            return new String[]{UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString()};
        } else {
            throw new AssertionError("Invalid field type: " + cls);
        }
    }
}
