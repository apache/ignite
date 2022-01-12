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

package org.apache.ignite.internal.network.serialization;

import static org.apache.ignite.internal.network.serialization.BuiltinType.ARRAY_LIST;
import static org.apache.ignite.internal.network.serialization.BuiltinType.BARE_OBJECT;
import static org.apache.ignite.internal.network.serialization.BuiltinType.BIT_SET;
import static org.apache.ignite.internal.network.serialization.BuiltinType.BOOLEAN;
import static org.apache.ignite.internal.network.serialization.BuiltinType.BOOLEAN_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltinType.BOOLEAN_BOXED;
import static org.apache.ignite.internal.network.serialization.BuiltinType.BYTE;
import static org.apache.ignite.internal.network.serialization.BuiltinType.BYTE_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltinType.BYTE_BOXED;
import static org.apache.ignite.internal.network.serialization.BuiltinType.CHAR;
import static org.apache.ignite.internal.network.serialization.BuiltinType.CHAR_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltinType.CHAR_BOXED;
import static org.apache.ignite.internal.network.serialization.BuiltinType.DATE;
import static org.apache.ignite.internal.network.serialization.BuiltinType.DECIMAL;
import static org.apache.ignite.internal.network.serialization.BuiltinType.DECIMAL_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltinType.DOUBLE;
import static org.apache.ignite.internal.network.serialization.BuiltinType.DOUBLE_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltinType.DOUBLE_BOXED;
import static org.apache.ignite.internal.network.serialization.BuiltinType.ENUM;
import static org.apache.ignite.internal.network.serialization.BuiltinType.ENUM_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltinType.FLOAT;
import static org.apache.ignite.internal.network.serialization.BuiltinType.FLOAT_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltinType.FLOAT_BOXED;
import static org.apache.ignite.internal.network.serialization.BuiltinType.HASH_MAP;
import static org.apache.ignite.internal.network.serialization.BuiltinType.HASH_SET;
import static org.apache.ignite.internal.network.serialization.BuiltinType.IGNITE_UUID;
import static org.apache.ignite.internal.network.serialization.BuiltinType.INT;
import static org.apache.ignite.internal.network.serialization.BuiltinType.INT_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltinType.INT_BOXED;
import static org.apache.ignite.internal.network.serialization.BuiltinType.LINKED_HASH_MAP;
import static org.apache.ignite.internal.network.serialization.BuiltinType.LINKED_HASH_SET;
import static org.apache.ignite.internal.network.serialization.BuiltinType.LINKED_LIST;
import static org.apache.ignite.internal.network.serialization.BuiltinType.LONG;
import static org.apache.ignite.internal.network.serialization.BuiltinType.LONG_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltinType.LONG_BOXED;
import static org.apache.ignite.internal.network.serialization.BuiltinType.NULL;
import static org.apache.ignite.internal.network.serialization.BuiltinType.OBJECT_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltinType.SHORT;
import static org.apache.ignite.internal.network.serialization.BuiltinType.SHORT_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltinType.SHORT_BOXED;
import static org.apache.ignite.internal.network.serialization.BuiltinType.SINGLETON_LIST;
import static org.apache.ignite.internal.network.serialization.BuiltinType.STRING;
import static org.apache.ignite.internal.network.serialization.BuiltinType.STRING_ARRAY;
import static org.apache.ignite.internal.network.serialization.BuiltinType.UUID;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * Tests for default descriptors.
 */
public class DefaultDescriptorsTest {
    /**
     * Tests that default descriptor were not changed by accident.
     */
    @Test
    public void testStableDescriptorIds() {
        assertEquals(0, BYTE.descriptorId());
        assertEquals(1, BYTE_BOXED.descriptorId());
        assertEquals(2, SHORT.descriptorId());
        assertEquals(3, SHORT_BOXED.descriptorId());
        assertEquals(4, INT.descriptorId());
        assertEquals(5, INT_BOXED.descriptorId());
        assertEquals(6, FLOAT.descriptorId());
        assertEquals(7, FLOAT_BOXED.descriptorId());
        assertEquals(8, LONG.descriptorId());
        assertEquals(9, LONG_BOXED.descriptorId());
        assertEquals(10, DOUBLE.descriptorId());
        assertEquals(11, DOUBLE_BOXED.descriptorId());
        assertEquals(12, BOOLEAN.descriptorId());
        assertEquals(13, BOOLEAN_BOXED.descriptorId());
        assertEquals(14, CHAR.descriptorId());
        assertEquals(15, CHAR_BOXED.descriptorId());
        assertEquals(16, BARE_OBJECT.descriptorId());
        assertEquals(17, STRING.descriptorId());
        assertEquals(18, UUID.descriptorId());
        assertEquals(19, IGNITE_UUID.descriptorId());
        assertEquals(20, DATE.descriptorId());
        assertEquals(21, BYTE_ARRAY.descriptorId());
        assertEquals(22, SHORT_ARRAY.descriptorId());
        assertEquals(23, INT_ARRAY.descriptorId());
        assertEquals(24, FLOAT_ARRAY.descriptorId());
        assertEquals(25, LONG_ARRAY.descriptorId());
        assertEquals(26, DOUBLE_ARRAY.descriptorId());
        assertEquals(27, BOOLEAN_ARRAY.descriptorId());
        assertEquals(28, CHAR_ARRAY.descriptorId());
        assertEquals(29, OBJECT_ARRAY.descriptorId());
        assertEquals(30, STRING_ARRAY.descriptorId());
        assertEquals(31, DECIMAL.descriptorId());
        assertEquals(32, DECIMAL_ARRAY.descriptorId());
        assertEquals(33, ENUM.descriptorId());
        assertEquals(34, ENUM_ARRAY.descriptorId());
        assertEquals(35, ARRAY_LIST.descriptorId());
        assertEquals(36, LINKED_LIST.descriptorId());
        assertEquals(37, HASH_SET.descriptorId());
        assertEquals(38, LINKED_HASH_SET.descriptorId());
        assertEquals(39, SINGLETON_LIST.descriptorId());
        assertEquals(40, HASH_MAP.descriptorId());
        assertEquals(41, LINKED_HASH_MAP.descriptorId());
        assertEquals(42, BIT_SET.descriptorId());
        assertEquals(43, NULL.descriptorId());
    }
}
