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
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.BooleanInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.ByteInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.BytesInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.DateInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.DoubleInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.FloatInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.IntegerInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.LongInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.ObjectHashInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.ShortInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.StringInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.TimeInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.TimestampInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.UuidInlineIndexKeyType;
import org.apache.ignite.internal.binary.BinaryObjectImpl;

/**
 * Provide mapping for java types and {@link IndexKeyTypes} that supports inlining.
 */
public class InlineIndexKeyTypeRegistry {
    /** Class mapping. */
    private static final Map<Class<?>, InlineIndexKeyType> classMapping = new HashMap<>();

    /** Type mapping. */
    private static final Map<Integer, InlineIndexKeyType> typeMapping = new HashMap<>();

    /** Object key type does not map to known java type. */
    private static final  ObjectHashInlineIndexKeyType objectType = new ObjectHashInlineIndexKeyType();

    static {
        classMapping.put(Boolean.class, new BooleanInlineIndexKeyType());
        classMapping.put(Byte.class, new ByteInlineIndexKeyType());
        classMapping.put(byte[].class, new BytesInlineIndexKeyType());
        classMapping.put(Date.class, new DateInlineIndexKeyType());
        classMapping.put(Double.class, new DoubleInlineIndexKeyType());
        classMapping.put(Float.class, new FloatInlineIndexKeyType());
        classMapping.put(Integer.class, new IntegerInlineIndexKeyType());
        classMapping.put(Long.class, new LongInlineIndexKeyType());
        classMapping.put(Short.class, new ShortInlineIndexKeyType());
        // TODO: ignore case handling
        classMapping.put(String.class, new StringInlineIndexKeyType(false));
        classMapping.put(Time.class, new TimeInlineIndexKeyType());
        classMapping.put(Timestamp.class, new TimestampInlineIndexKeyType());
        classMapping.put(UUID.class, new UuidInlineIndexKeyType());

        for (InlineIndexKeyType cm: classMapping.values())
            typeMapping.put(cm.type(), cm);

        typeMapping.put(IndexKeyTypes.JAVA_OBJECT, new ObjectHashInlineIndexKeyType());
    }

    /**
     * Get key type for a class. Used for user queries, where getting type from class.
     */
    public static InlineIndexKeyType get(Class<?> clazz) {
        // TODO: more keys?
        if (clazz == BinaryObjectImpl.class)
            return objectType;

        InlineIndexKeyType key = classMapping.get(clazz);

        if (key == null)
            throw new IgniteException("There is no InlineIndexKey mapping for class " + clazz);

        return key;
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
        InlineIndexKeyType indexKeyType = typeMapping.get(type);

        if (indexKeyType == null)
            throw new IgniteException("Type does not support inlining: " + type);

        return indexKeyType;
    }
}
