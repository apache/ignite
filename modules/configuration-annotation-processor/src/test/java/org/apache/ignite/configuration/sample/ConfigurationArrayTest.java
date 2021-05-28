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

package org.apache.ignite.configuration.sample;

import java.util.function.Supplier;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.internal.asm.ConfigurationAsmGenerator;
import org.apache.ignite.configuration.tree.InnerNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.apache.ignite.configuration.internal.util.ConfigurationUtil.leafNodeVisitor;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/**
 * Test configuration with array of primitive type (or {@link String}) fields.
 */
public class ConfigurationArrayTest {
    /** */
    @Config
    public static class TestArrayConfigurationSchema {
        /** */
        @Value
        public String[] array;
    }

    private static ConfigurationAsmGenerator cgen;

    @BeforeAll
    public static void beforeAll() {
        cgen = new ConfigurationAsmGenerator();

        cgen.compileRootSchema(TestArrayConfigurationSchema.class);
    }

    @AfterAll
    public static void afterAll() {
        cgen = null;
    }

    /**
     * Test array node init operation.
     */
    @Test
    public void testInit() {
        InnerNode arrayNode = cgen.instantiateNode(TestArrayConfigurationSchema.class);

        Supplier<String[]> initialSupplier = () -> {
            return new String[]{"test1", "test2"};
        };

        final String[] initialValue = initialSupplier.get();
        ((TestArrayChange)arrayNode).changeArray(initialValue);

        // test that field is not the same as initialValue
        assertNotSame(getArray(arrayNode), initialValue);

        // test that init method set values successfully
        assertThat(((TestArrayView)arrayNode).array(), is(initialSupplier.get()));

        // test that returned array is a copy of the field
        assertNotSame(getArray(arrayNode), ((TestArrayView)arrayNode).array());
    }

    /**
     * Test array node change operation.
     */
    @Test
    public void testChange() {
        InnerNode arrayNode = cgen.instantiateNode(TestArrayConfigurationSchema.class);

        Supplier<String[]> changeSupplier = () -> {
            return new String[]{"foo", "bar"};
        };

        final String[] changeValue = changeSupplier.get();
        ((TestArrayChange)arrayNode).changeArray(changeValue);

        // test that field is not the same as initialValue
        assertNotSame(getArray(arrayNode), changeValue);

        // test that change method set values successfully
        assertThat(((TestArrayView)arrayNode).array(), is(changeSupplier.get()));

        // test that returned array is a copy of the field
        assertNotSame(getArray(arrayNode), ((TestArrayView)arrayNode).array());
    }

    /**
     * Get array field from ArrayNode
     * @param arrayNode ArrayNode.
     * @return Array field value.
     */
    private String[] getArray(InnerNode arrayNode) {
        return (String[])arrayNode.traverseChild("array", leafNodeVisitor());
    }
}
