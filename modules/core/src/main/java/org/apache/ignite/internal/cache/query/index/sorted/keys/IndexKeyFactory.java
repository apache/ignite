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

package org.apache.ignite.internal.cache.query.index.sorted.keys;

import java.math.BigDecimal;
import java.util.EnumMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyType;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypeSettings;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;

/**
 * Factory for creating IndexKey objects.
 */
public class IndexKeyFactory {
    /** Registry for non-default key types factory methods (e.g., Geometry). */
    private static final Map<IndexKeyType, Function<Object, IndexKey>> registry = new EnumMap<>(IndexKeyType.class);

    /** Register wrapper for custom IndexKey type. Used by Ignite extensions. */
    public static void register(IndexKeyType keyType, Function<Object, IndexKey> wrapper) {
        registry.put(keyType, wrapper);
    }

    /** Wraps user object to {@code IndexKey} object.  */
    public static IndexKey wrap(Object o, int keyType, CacheObjectValueContext coctx, IndexKeyTypeSettings keyTypeSettings) {
        return wrap(o, IndexKeyType.forCode(keyType), coctx, keyTypeSettings);
    }

    /** Wraps user object to {@code IndexKey} object.  */
    public static IndexKey wrap(Object o, IndexKeyType keyType, CacheObjectValueContext coctx, IndexKeyTypeSettings keyTypeSettings) {
        if (o == null || keyType == IndexKeyType.NULL)
            return NullIndexKey.INSTANCE;

        switch (keyType) {
            case BOOLEAN:
                return new BooleanIndexKey((boolean)o);
            case BYTE:
                return new ByteIndexKey((byte)o);
            case SHORT:
                return new ShortIndexKey((short)o);
            case INT:
                return new IntegerIndexKey((int)o);
            case LONG:
                return new LongIndexKey((long)o);
            case DECIMAL:
                return new DecimalIndexKey((BigDecimal)o);
            case DOUBLE:
                return new DoubleIndexKey((double)o);
            case FLOAT:
                return new FloatIndexKey((float)o);
            case BYTES:
                return keyTypeSettings.binaryUnsigned() ?
                    new BytesIndexKey((byte[])o) : new SignedBytesIndexKey((byte[])o);
            case STRING:
                return new StringIndexKey((String)o);
            case UUID:
                return new UuidIndexKey((UUID)o);
            case JAVA_OBJECT:
                if (BinaryUtils.isBinaryObjectImpl(o))
                    return new CacheJavaObjectIndexKey((CacheObject)o, coctx);

                return new PlainJavaObjectIndexKey(o, null);
            case DATE:
                return new DateIndexKey(o);
            case TIME:
                return new TimeIndexKey(o);
            case TIMESTAMP:
                return new TimestampIndexKey(o);
        }

        if (registry.containsKey(keyType))
            return registry.get(keyType).apply(o);

        throw new IgniteException("Failed to wrap value [type=" + keyType + ", value=" + o + "]");
    }
}
