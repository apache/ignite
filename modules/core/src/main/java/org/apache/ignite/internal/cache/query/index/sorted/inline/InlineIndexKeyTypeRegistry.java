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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyDefinition;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.BooleanInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.ByteInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.BytesInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.DateInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.DoubleInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.FloatInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.IntegerInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.LongInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.ObjectByteArrayInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.ObjectHashInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.ShortInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.SignedBytesInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.StringInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.StringNoCompareInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.TimeInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.TimestampInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.inline.types.UuidInlineIndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.keys.IndexKey;
import org.apache.ignite.internal.cache.query.index.sorted.keys.NullIndexKey;

/**
 * Provide mapping for java types and {@link IndexKeyType} that supports inlining.
 */
public class InlineIndexKeyTypeRegistry {
    /** Type mapping. */
    private static final Map<IndexKeyType, InlineIndexKeyType> typeMapping = new EnumMap<>(IndexKeyType.class);

    /** Object key type that maps for custom POJO. Inline stores a hash of the object. */
    private static final ObjectHashInlineIndexKeyType hashObjectType = new ObjectHashInlineIndexKeyType();

    /** Default String key type use optimized algorithm for comparison. */
    private static final StringInlineIndexKeyType optimizedCompareStringType = new StringInlineIndexKeyType();

    /** Do not compare inlined String keys. */
    private static final StringNoCompareInlineIndexKeyType noCompareStringType = new StringNoCompareInlineIndexKeyType();

    /** Default String key type use optimized algorithm for comparison. */
    private static final BytesInlineIndexKeyType bytesType = new BytesInlineIndexKeyType();

    /** Do not compare inlined String keys. */
    private static final SignedBytesInlineIndexKeyType signedBytesType = new SignedBytesInlineIndexKeyType();

    /** Object key type that maps for custom POJO. Inline stores a byte array representation of the object. */
    private static final ObjectByteArrayInlineIndexKeyType bytesObjectType =
        new ObjectByteArrayInlineIndexKeyType(new BytesInlineIndexKeyType(IndexKeyType.JAVA_OBJECT));

    /** Object key type that maps for custom POJO. Inline stores a signed byte array representation of the object. */
    private static final ObjectByteArrayInlineIndexKeyType signedBytesObjectType =
        new ObjectByteArrayInlineIndexKeyType(new SignedBytesInlineIndexKeyType(IndexKeyType.JAVA_OBJECT));

    static {
        register(IndexKeyType.BOOLEAN, new BooleanInlineIndexKeyType());
        register(IndexKeyType.BYTE, new ByteInlineIndexKeyType());
        register(IndexKeyType.DATE, new DateInlineIndexKeyType());
        register(IndexKeyType.DOUBLE, new DoubleInlineIndexKeyType());
        register(IndexKeyType.FLOAT, new FloatInlineIndexKeyType());
        register(IndexKeyType.INT, new IntegerInlineIndexKeyType());
        register(IndexKeyType.SHORT, new ShortInlineIndexKeyType());
        register(IndexKeyType.LONG, new LongInlineIndexKeyType());
        register(IndexKeyType.TIME, new TimeInlineIndexKeyType());
        register(IndexKeyType.TIMESTAMP, new TimestampInlineIndexKeyType());
        register(IndexKeyType.UUID, new UuidInlineIndexKeyType());
        // Choice of those types actually depends on IndexKeyTypeSettings.
        register(IndexKeyType.JAVA_OBJECT, hashObjectType);
        register(IndexKeyType.STRING, optimizedCompareStringType);
        register(IndexKeyType.BYTES, bytesType);
    }

    /** */
    private static void register(IndexKeyType type, InlineIndexKeyType keyType) {
        typeMapping.put(type, keyType);
    }

    /**
     * Get key type for a class. Used for user queries, where getting type from class.
     * Type is required for cases when class doesn't have strict type relation (nulls, POJO).
     *
     * @param expType Expected type of a key.
     */
    public static InlineIndexKeyType get(IndexKeyType expType, IndexKeyTypeSettings keyTypeSettings) {
        return type(expType, keyTypeSettings);
    }

    /**
     * Get key type for specified key. Used for user queries, where getting type from class.
     * Type is required for cases when class doesn't have strict type relation (nulls, POJO).
     *
     * @param key Index key.
     * @param expType Expected type of a key.
     * @param keyTypeSettings Index key type settings.
     */
    public static InlineIndexKeyType get(IndexKey key, IndexKeyType expType, IndexKeyTypeSettings keyTypeSettings) {
        return key == NullIndexKey.INSTANCE ?
            type(expType, keyTypeSettings) :
            type(key.type(), keyTypeSettings);
    }

    /** */
    private static InlineIndexKeyType type(IndexKeyType type, IndexKeyTypeSettings keyTypeSettings) {
        if (type == IndexKeyType.JAVA_OBJECT)
            return javaObjectType(keyTypeSettings);

        else if (type == IndexKeyType.STRING)
            return stringType(keyTypeSettings);

        else if (type == IndexKeyType.BYTES)
            return bytesType(keyTypeSettings);

        return typeMapping.get(type);
    }

    /**
     * Checks whether specified type support inlining.
     */
    private static boolean supportInline(IndexKeyType type, IndexKeyTypeSettings keyTypeSettings) {
        if (type == IndexKeyType.JAVA_OBJECT && !keyTypeSettings.inlineObjSupported())
            return false;

        return typeMapping.containsKey(type);
    }

    /**
     * Get key type for the POJO type.
     */
    private static InlineIndexKeyType javaObjectType(IndexKeyTypeSettings keyTypeSettings) {
        if (keyTypeSettings.inlineObjHash())
            return hashObjectType;

        return keyTypeSettings.binaryUnsigned() ? bytesObjectType : signedBytesObjectType;
    }

    /**
     * Get key type for the String type.
     */
    private static InlineIndexKeyType stringType(IndexKeyTypeSettings keyTypeSettings) {
        return keyTypeSettings.stringOptimizedCompare() ? optimizedCompareStringType : noCompareStringType;
    }

    /**
     * Get key type for the Bytes type.
     */
    private static InlineIndexKeyType bytesType(IndexKeyTypeSettings keyTypeSettings) {
        return keyTypeSettings.binaryUnsigned() ? bytesType : signedBytesType;
    }

    /**
     * Return list of key types for specified key definitions and key type settings.
     */
    public static List<InlineIndexKeyType> types(Collection<IndexKeyDefinition> keyDefs, IndexKeyTypeSettings settings) {
        List<InlineIndexKeyType> keyTypes = new ArrayList<>();

        for (IndexKeyDefinition keyDef: keyDefs) {
            if (!supportInline(keyDef.idxType(), settings))
                break;

            keyTypes.add(type(keyDef.idxType(), settings));
        }

        return Collections.unmodifiableList(keyTypes);
    }
}
