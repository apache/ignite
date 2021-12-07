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

import static org.apache.ignite.internal.network.serialization.DefaultType.ARRAY_LIST;
import static org.apache.ignite.internal.network.serialization.DefaultType.BIT_SET;
import static org.apache.ignite.internal.network.serialization.DefaultType.BOOLEAN;
import static org.apache.ignite.internal.network.serialization.DefaultType.BOOLEAN_ARRAY;
import static org.apache.ignite.internal.network.serialization.DefaultType.BOOLEAN_BOXED;
import static org.apache.ignite.internal.network.serialization.DefaultType.BYTE;
import static org.apache.ignite.internal.network.serialization.DefaultType.BYTE_ARRAY;
import static org.apache.ignite.internal.network.serialization.DefaultType.BYTE_BOXED;
import static org.apache.ignite.internal.network.serialization.DefaultType.CHAR;
import static org.apache.ignite.internal.network.serialization.DefaultType.CHAR_ARRAY;
import static org.apache.ignite.internal.network.serialization.DefaultType.CHAR_BOXED;
import static org.apache.ignite.internal.network.serialization.DefaultType.DATE;
import static org.apache.ignite.internal.network.serialization.DefaultType.DECIMAL;
import static org.apache.ignite.internal.network.serialization.DefaultType.DECIMAL_ARRAY;
import static org.apache.ignite.internal.network.serialization.DefaultType.DOUBLE;
import static org.apache.ignite.internal.network.serialization.DefaultType.DOUBLE_ARRAY;
import static org.apache.ignite.internal.network.serialization.DefaultType.DOUBLE_BOXED;
import static org.apache.ignite.internal.network.serialization.DefaultType.ENUM;
import static org.apache.ignite.internal.network.serialization.DefaultType.ENUM_ARRAY;
import static org.apache.ignite.internal.network.serialization.DefaultType.FLOAT;
import static org.apache.ignite.internal.network.serialization.DefaultType.FLOAT_ARRAY;
import static org.apache.ignite.internal.network.serialization.DefaultType.FLOAT_BOXED;
import static org.apache.ignite.internal.network.serialization.DefaultType.HASH_MAP;
import static org.apache.ignite.internal.network.serialization.DefaultType.HASH_SET;
import static org.apache.ignite.internal.network.serialization.DefaultType.IGNITE_UUID;
import static org.apache.ignite.internal.network.serialization.DefaultType.INT;
import static org.apache.ignite.internal.network.serialization.DefaultType.INT_ARRAY;
import static org.apache.ignite.internal.network.serialization.DefaultType.INT_BOXED;
import static org.apache.ignite.internal.network.serialization.DefaultType.LINKED_HASH_MAP;
import static org.apache.ignite.internal.network.serialization.DefaultType.LINKED_HASH_SET;
import static org.apache.ignite.internal.network.serialization.DefaultType.LINKED_LIST;
import static org.apache.ignite.internal.network.serialization.DefaultType.LONG;
import static org.apache.ignite.internal.network.serialization.DefaultType.LONG_ARRAY;
import static org.apache.ignite.internal.network.serialization.DefaultType.LONG_BOXED;
import static org.apache.ignite.internal.network.serialization.DefaultType.NULL;
import static org.apache.ignite.internal.network.serialization.DefaultType.OBJECT;
import static org.apache.ignite.internal.network.serialization.DefaultType.OBJECT_ARRAY;
import static org.apache.ignite.internal.network.serialization.DefaultType.SHORT;
import static org.apache.ignite.internal.network.serialization.DefaultType.SHORT_ARRAY;
import static org.apache.ignite.internal.network.serialization.DefaultType.SHORT_BOXED;
import static org.apache.ignite.internal.network.serialization.DefaultType.SINGLETON_LIST;
import static org.apache.ignite.internal.network.serialization.DefaultType.STRING;
import static org.apache.ignite.internal.network.serialization.DefaultType.STRING_ARRAY;
import static org.apache.ignite.internal.network.serialization.DefaultType.UUID;
import static org.apache.ignite.internal.network.serialization.DefaultType.VOID;
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
        assertEquals(16, OBJECT.descriptorId());
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
        assertEquals(44, VOID.descriptorId());
    }
}
