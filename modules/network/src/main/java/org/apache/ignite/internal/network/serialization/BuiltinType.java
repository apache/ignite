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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import org.apache.ignite.lang.IgniteUuid;

/**
 * Built-in types.
 *
 * <p>They share ID space with commands defined in {@link SerializedStreamCommands}.
 */
public enum BuiltinType {
    BYTE(0, byte.class),
    BYTE_BOXED(1, Byte.class),
    SHORT(2, short.class),
    SHORT_BOXED(3, Short.class),
    INT(4, int.class),
    INT_BOXED(5, Integer.class),
    FLOAT(6, float.class),
    FLOAT_BOXED(7, Float.class),
    LONG(8, long.class),
    LONG_BOXED(9, Long.class),
    DOUBLE(10, double.class),
    DOUBLE_BOXED(11, Double.class),
    BOOLEAN(12, boolean.class),
    BOOLEAN_BOXED(13, Boolean.class),
    CHAR(14, char.class),
    CHAR_BOXED(15, Character.class),
    /** An object whose class is exactly Object (not a subclass). It is created using new Object(). */
    BARE_OBJECT(16, Object.class),
    STRING(17, String.class),
    UUID(18, java.util.UUID.class),
    IGNITE_UUID(19, IgniteUuid.class),
    DATE(20, Date.class),
    BYTE_ARRAY(21, byte[].class),
    SHORT_ARRAY(22, short[].class),
    INT_ARRAY(23, int[].class),
    FLOAT_ARRAY(24, float[].class),
    LONG_ARRAY(25, long[].class),
    DOUBLE_ARRAY(26, double[].class),
    BOOLEAN_ARRAY(27, boolean[].class),
    CHAR_ARRAY(28, char[].class),
    /** An arbitrary array of objects (references) except for the cases supported specifically (like String[], Enum{} and so on). */
    OBJECT_ARRAY(29, Object[].class),
    STRING_ARRAY(30, String[].class),
    DECIMAL(31, BigDecimal.class),
    DECIMAL_ARRAY(32, BigDecimal[].class),
    ENUM(33, Enum.class),
    ENUM_ARRAY(34, Enum[].class),
    ARRAY_LIST(35, ArrayList.class),
    LINKED_LIST(36, LinkedList.class),
    HASH_SET(37, HashSet.class),
    LINKED_HASH_SET(38, LinkedHashSet.class),
    SINGLETON_LIST(39, Collections.singletonList(null).getClass()),
    HASH_MAP(40, HashMap.class),
    LINKED_HASH_MAP(41, LinkedHashMap.class),
    BIT_SET(42, BitSet.class),
    NULL(43, Null.class)
    // 44 is REFERENCE command, see SerializedStreamCommands#REFERENCE
    ;

    /**
     * Pre-defined descriptor id.
     */
    private final int descriptorId;

    /**
     * Type.
     */
    private final Class<?> clazz;

    /**
     * Constructor.
     *
     * @param descriptorId Descriptor id.
     * @param clazz        Type.
     */
    BuiltinType(int descriptorId, Class<?> clazz) {
        this.descriptorId = descriptorId;
        this.clazz = clazz;
    }

    /**
     * Returns descriptor id.
     *
     * @return Descriptor id.
     */
    public int descriptorId() {
        return descriptorId;
    }

    /**
     * Returns type of the descriptor.
     *
     * @return Type.
     */
    public Class<?> clazz() {
        return clazz;
    }

    /**
     * Creates a descriptor for this built-in type.
     *
     * @return Descriptor.
     */
    public ClassDescriptor asClassDescriptor() {
        return new ClassDescriptor(
                clazz,
                descriptorId,
                null,
                Collections.emptyList(),
                new Serialization(SerializationType.BUILTIN)
        );
    }
}
