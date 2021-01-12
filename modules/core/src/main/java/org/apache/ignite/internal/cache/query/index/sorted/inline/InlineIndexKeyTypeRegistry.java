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

package org.apache.ignite.internal.cache.query.index.sorted.inline;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.cache.query.index.sorted.JavaObjectKey;
import org.apache.ignite.internal.cache.query.index.sorted.NullKey;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.BooleanInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.ByteInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.BytesInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.DateInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.DoubleInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.FloatInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.IntegerInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.LegacyDateInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.LongInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.ObjectHashInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.ShortInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.StringInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.TimeInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.TimestampInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.UuidInlineIndexKeyType;

/**
 * Provide mapping for java types and {@link IndexKeyTypes} that supports inlining.
 */
public class InlineIndexKeyTypeRegistry {
    /** Class mapping. */
    private static final Map<Class<?>, InlineIndexKeyType> classMapping = new HashMap<>();

    /** Type mapping. */
    private static final Map<Integer, InlineIndexKeyType> typeMapping = new HashMap<>();

    /** Object key type does not map to known java type. */
    private static final ObjectHashInlineIndexKeyType objectType = new ObjectHashInlineIndexKeyType();

    static {
        classMapping.put(Boolean.class, new BooleanInlineIndexKeyType());
        classMapping.put(Byte.class, new ByteInlineIndexKeyType());
        classMapping.put(byte[].class, new BytesInlineIndexKeyType());
        classMapping.put(Date.class, new DateInlineIndexKeyType());
        classMapping.put(Timestamp.class, new TimestampInlineIndexKeyType());
        classMapping.put(java.util.Date.class, new LegacyDateInlineIndexKeyType());
        classMapping.put(Double.class, new DoubleInlineIndexKeyType());
        classMapping.put(Float.class, new FloatInlineIndexKeyType());
        classMapping.put(Integer.class, new IntegerInlineIndexKeyType());
        classMapping.put(Long.class, new LongInlineIndexKeyType());
        classMapping.put(Short.class, new ShortInlineIndexKeyType());
        // TODO: ignore case handling
        classMapping.put(String.class, new StringInlineIndexKeyType(false));
        classMapping.put(Time.class, new TimeInlineIndexKeyType());
        classMapping.put(UUID.class, new UuidInlineIndexKeyType());

        for (InlineIndexKeyType cm: classMapping.values())
            typeMapping.put(cm.type(), cm);

        typeMapping.put(IndexKeyTypes.JAVA_OBJECT, objectType);
    }

    /**
     * Get key type for a class. Used for user queries, where getting type from class.
     * Type is required for cases when class doesn't have strict type relation (nulls, POJO).
     *
     * @param clazz Class of a key.
     * @param type Type of a key.
     */
    public static InlineIndexKeyType get(Class<?> clazz, int type) {
        if (clazz == NullKey.class)
            return get(type);

        InlineIndexKeyType key = classMapping.get(clazz);

        if (key == null && type == IndexKeyTypes.JAVA_OBJECT)
            return objectType;

        if (key == null && clazz == BinaryObjectImpl.class)
            return objectType;

        return key == null ? get(type) : key;
    }

    /**
     * Checks whether specified type support inlining.
     */
    public static boolean supportInline(int type) {
        return typeMapping.containsKey(type);
    }

    /**
     * Get key type for a specified type. Should check {@link #supportInline(int)} before invoke it.
     */
    public static InlineIndexKeyType get(int type) {
        InlineIndexKeyType idxKeyType = typeMapping.get(type);

        if (idxKeyType == null)
            throw new IgniteException("Type does not support inlining: " + type);

        return idxKeyType;
    }

    /**
     * Validates that specified type and specified class are the same InlineIndexKeyType.
     */
    public static boolean validate(int type, Class<?> clazz) {
        if (clazz == NullKey.class)
            return true;

        if (clazz == BinaryObjectImpl.class || clazz == JavaObjectKey.class)
            return type == IndexKeyTypes.JAVA_OBJECT;

        InlineIndexKeyType key = classMapping.get(clazz);

        if (key == null)
            throw new IgniteException("There is no InlineIndexKey mapping for class " + clazz);

        return typeMapping.get(type).type() == key.type();
    }
}
