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
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.binary.BinaryObjectImpl;
import org.apache.ignite.internal.cache.query.index.sorted.IndexKeyTypes;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;

/** */
public class IndexKeyRegistry {
    /** Registry for non-default key types (e.g., Geometry). */
    private static final Map<Integer, Function<Object, IndexKey>> registry = new HashMap<>();

    /** */
    public static void register(int keyType, Function<Object, IndexKey> wrapper) {
        registry.put(keyType, wrapper);
    }

    /** Wraps user object to {@code IndexKey} object.  */
    public static IndexKey wrap(Object o, int keyType, CacheObjectValueContext coctx) {
        if (o == null || keyType == IndexKeyTypes.NULL)
            return NullIndexKey.INSTANCE;

        switch (keyType) {
            case IndexKeyTypes.BOOLEAN:
                return new BooleanIndexKey((boolean) o);
            case IndexKeyTypes.BYTE:
                return new ByteIndexKey((byte) o);
            case IndexKeyTypes.SHORT:
                return new ShortIndexKey((short) o);
            case IndexKeyTypes.INT:
                return new IntegerIndexKey((int) o);
            case IndexKeyTypes.LONG:
                return new LongIndexKey((long) o);
            case IndexKeyTypes.DECIMAL:
                return new DecimalIndexKey((BigDecimal) o);
            case IndexKeyTypes.DOUBLE:
                return new DoubleIndexKey((double) o);
            case IndexKeyTypes.FLOAT:
                return new FloatIndexKey((float) o);

            case IndexKeyTypes.TIME:
                if (java.time.LocalTime.class == o.getClass())
                    return new TimeIndexKey((LocalTime) o);

                return new TimeIndexKey((Time) o);

            case IndexKeyTypes.DATE:
                if (java.time.LocalDate.class == o.getClass())
                    return new DateIndexKey((java.time.LocalDate) o);

                return new DateIndexKey((java.sql.Date) o);

            case IndexKeyTypes.TIMESTAMP:
                if (o instanceof java.util.Date && !(o instanceof Timestamp))
                    o = new Timestamp(((java.util.Date) o).getTime());

                if (java.time.LocalDateTime.class == o.getClass())
                    return new TimestampIndexKey((java.time.LocalDateTime) o);

                return new TimestampIndexKey((Timestamp) o);

            case IndexKeyTypes.BYTES:
                return new BytesIndexKey((byte[]) o);
            case IndexKeyTypes.STRING:
                return new StringIndexKey((String) o);
            case IndexKeyTypes.UUID:
                return new UuidIndexKey((UUID) o);
            case IndexKeyTypes.JAVA_OBJECT:
                if (BinaryObjectImpl.class == o.getClass())
                    return new CacheJavaObjectIndexKey((CacheObject) o, coctx);

                return new PlainJavaObjectIndexKey(o, null);
        }

        if (registry.containsKey(keyType))
            return registry.get(keyType).apply(o);

        throw new IgniteException("Failed to wrap value[type=" + keyType + ", value=" + o + "]");
    }
}
