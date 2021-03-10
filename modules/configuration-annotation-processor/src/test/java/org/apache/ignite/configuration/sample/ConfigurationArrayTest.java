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

import java.io.Serializable;
import java.util.function.Supplier;
import org.apache.ignite.configuration.annotation.Config;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.sample.impl.TestArrayNode;
import org.apache.ignite.configuration.tree.ConfigurationVisitor;
import org.junit.jupiter.api.Test;

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

    /**
     * Test array node init operation.
     */
    @Test
    public void testInit() {
        var arrayNode = new TestArrayNode();

        Supplier<String[]> initialSupplier = () -> {
            return new String[]{"test1", "test2"};
        };

        final String[] initialValue = initialSupplier.get();
        arrayNode.initArray(initialValue);

        // test that field is not the same as initialValue
        assertNotSame(getArray(arrayNode), initialValue);

        // test that init method set values successfully
        assertThat(arrayNode.array(), is(initialSupplier.get()));

        // test that returned array is a copy of the field
        assertNotSame(getArray(arrayNode), arrayNode.array());
    }

    /**
     * Test array node change operation.
     */
    @Test
    public void testChange() {
        var arrayNode = new TestArrayNode();

        Supplier<String[]> changeSupplier = () -> {
            return new String[]{"foo", "bar"};
        };

        final String[] changeValue = changeSupplier.get();
        arrayNode.changeArray(changeValue);

        // test that field is not the same as initialValue
        assertNotSame(getArray(arrayNode), changeValue);

        // test that change method set values successfully
        assertThat(arrayNode.array(), is(changeSupplier.get()));

        // test that returned array is a copy of the field
        assertNotSame(getArray(arrayNode), arrayNode.array());
    }

    /**
     * Get array field from ArrayNode
     * @param arrayNode ArrayNode.
     * @return Array field value.
     */
    private String[] getArray(TestArrayNode arrayNode) {
        return arrayNode.traverseChild("array", new ConfigurationVisitor<String[]>() {
            /** {@inheritDoc} */
            @Override public String[] visitLeafNode(String key, Serializable val) {
                return (String[]) val;
            }
        });
    }
}
