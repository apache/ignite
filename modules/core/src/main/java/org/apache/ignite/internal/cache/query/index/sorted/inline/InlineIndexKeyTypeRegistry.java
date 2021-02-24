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

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypes;
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
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.LocalDateInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.LocalDateTimeInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.LocalTimeInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.LongInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.ObjectByteArrayInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.ObjectHashInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.ShortInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.StringInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.keys.StringNoCompareInlineIndexKeyType;
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

    /** Object key type does not map to known java type. */
    private static final ObjectByteArrayInlineIndexKeyType legacyObjectType = new ObjectByteArrayInlineIndexKeyType();

    /** Default String key type use optimized algorithm for comparison. */
    private static final StringInlineIndexKeyType optimizedCompareStringType = new StringInlineIndexKeyType();

    /** Do not compare inlined String keys. */
    private static final StringNoCompareInlineIndexKeyType noCompareStringType = new StringNoCompareInlineIndexKeyType();

    static {
        register(IndexKeyTypes.BOOLEAN, new BooleanInlineIndexKeyType(), boolean.class, Boolean.class);
        register(IndexKeyTypes.BYTE, new ByteInlineIndexKeyType(), byte.class, Byte.class);
        register(IndexKeyTypes.SHORT, new ShortInlineIndexKeyType(), short.class, Short.class);
        register(IndexKeyTypes.INT, new IntegerInlineIndexKeyType(), int.class, Integer.class);
        register(IndexKeyTypes.LONG, new LongInlineIndexKeyType(), long.class, Long.class);
        register(IndexKeyTypes.DOUBLE, new DoubleInlineIndexKeyType(), double.class, Double.class);
        register(IndexKeyTypes.FLOAT, new FloatInlineIndexKeyType(), float.class, Float.class);
        register(IndexKeyTypes.TIME, new TimeInlineIndexKeyType(), java.sql.Time.class);
        register(IndexKeyTypes.TIME, new LocalTimeInlineIndexKeyType(), LocalTime.class);
        register(IndexKeyTypes.DATE, new DateInlineIndexKeyType(), java.sql.Date.class);
        register(IndexKeyTypes.DATE, new LocalDateInlineIndexKeyType(), LocalDate.class);
        register(IndexKeyTypes.TIMESTAMP, new LegacyDateInlineIndexKeyType(), java.util.Date.class);
        register(IndexKeyTypes.TIMESTAMP, new LocalDateTimeInlineIndexKeyType(), LocalDateTime.class);
        register(IndexKeyTypes.TIMESTAMP, new TimestampInlineIndexKeyType(), Timestamp.class);
        register(IndexKeyTypes.BYTES, new BytesInlineIndexKeyType(), byte[].class);
        register(IndexKeyTypes.UUID, new UuidInlineIndexKeyType(), UUID.class);
    }

    /** */
    private static void register(int type, InlineIndexKeyType keyType, Class<?>... classes) {
        typeMapping.put(type, keyType);

        for (Class<?> cls: classes)
            classMapping.put(cls, keyType);
    }

    /**
     * Get key type for a class. Used for user queries, where getting type from class.
     * Type is required for cases when class doesn't have strict type relation (nulls, POJO).
     *
     * @param clazz Class of a key.
     * @param expType Expected type of a key.
     */
    public static InlineIndexKeyType get(Class<?> clazz, int expType, IndexKeyTypeSettings keyTypeSettings) {
        boolean legacyObj = !keyTypeSettings.inlineObjHash();
        boolean optimizedCompareStr = keyTypeSettings.useStringOptimizedCompare();

        if (clazz == NullKey.class) {
            if (expType == IndexKeyTypes.JAVA_OBJECT)
                return getJavaObjectType(legacyObj);

            else if (expType == IndexKeyTypes.STRING)
                return getStringType(optimizedCompareStr);

            else
                // Actually it's wrong to get typeMapping due to collisions (Date, LocalDate classes map to single type).
                // But the object is null and then it's safe to use it as no type-specific code will be executed.
                // Also this approach returns correct type int value, it's enough.
                return typeMapping.get(expType);
        }

        // 1. BinaryObjectImpl.class is a class of stored POJO.
        // 2. Object.class is set up for custom (no-SQL) key types within IndexKeyDefinition.idxCls.
        if ((clazz == BinaryObjectImpl.class || clazz == Object.class))
            return getJavaObjectType(legacyObj);

        InlineIndexKeyType key = classMapping.get(clazz);

        // User defined POJO classes.
        if (key == null && expType == IndexKeyTypes.JAVA_OBJECT)
            return getJavaObjectType(legacyObj);

        if (key != null && key.type() == IndexKeyTypes.JAVA_OBJECT)
            return getJavaObjectType(legacyObj);

        if (key == null && expType == IndexKeyTypes.STRING)
            return getStringType(legacyObj);

        return key;
    }

    /**
     * Checks whether specified type support inlining.
     */
    public static boolean supportInline(int type) {
        return typeMapping.containsKey(type) || type == IndexKeyTypes.JAVA_OBJECT;
    }

    /**
     * Get key type for a POJO type.
     */
    private static InlineIndexKeyType getJavaObjectType(boolean legacyObj) {
        return legacyObj ? legacyObjectType : objectType;
    }

    /**
     * Get key type for a String type.
     */
    private static InlineIndexKeyType getStringType(boolean useOptimizedCompare) {
        return useOptimizedCompare ? optimizedCompareStringType : noCompareStringType;
    }

    /**
     * Validates that specified type and specified class are the same InlineIndexKeyType.
     */
    public static boolean validate(int type, Class<?> clazz) {
        if (clazz == NullKey.class)
            return true;

        if (clazz == BinaryObjectImpl.class || clazz == JavaObjectKey.class)
            return type == IndexKeyTypes.JAVA_OBJECT;

        if (clazz == String.class)
            return type == IndexKeyTypes.STRING;

        InlineIndexKeyType key = classMapping.get(clazz);

        if (key == null)
            throw new IgniteException("There is no InlineIndexKey mapping for class " + clazz);

        return typeMapping.get(type).type() == key.type();
    }

    /**
     * Return list of key types for specified key definitions and key type settings.
     * */
    public static List<InlineIndexKeyType> getTypes(List<IndexKeyDefinition> keyDefs, IndexKeyTypeSettings settings) {
        List<InlineIndexKeyType> keyTypes = new ArrayList<>();

        for (IndexKeyDefinition keyDef: keyDefs) {
            if (!supportInline(keyDef.getIdxType()))
                break;

            keyTypes.add(get(keyDef.getIdxClass(), keyDef.getIdxType(), settings));
        }

        return Collections.unmodifiableList(keyTypes);
    }
}
