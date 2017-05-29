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

package org.apache.ignite.internal.processors.cache;

import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.util.typedef.F;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/**
 * Cache object utility methods.
 */
public class CacheObjectUtils {
    /**
     * @param o Object to unwrap.
     * @param keepBinary Keep binary flag.
     * @param cpy Copy value flag.
     * @return Unwrapped object.
     */
    public static Object unwrapBinaryIfNeeded(CacheObjectValueContext ctx, Object o, boolean keepBinary, boolean cpy) {
        if (o == null)
            return null;

        return unwrapBinary(ctx, o, keepBinary, cpy);
    }

    /**
     * @param col Collection of objects to unwrap.
     * @param keepBinary Keep binary flag.
     * @return Unwrapped collection.
     */
    public static Collection<Object> unwrapBinariesIfNeeded(CacheObjectValueContext ctx, Collection<Object> col,
        boolean keepBinary) {
        return unwrapBinariesIfNeeded(ctx, col, keepBinary, true);
    }

    /**
     * @param col Collection to unwrap.
     * @param keepBinary Keep binary flag.
     * @param cpy Copy flag.
     * @return Unwrapped collection.
     */
    private static Collection<Object> unwrapKnownCollection(CacheObjectValueContext ctx, Collection<Object> col,
        boolean keepBinary, boolean cpy) {
        Collection<Object> col0 = BinaryUtils.newKnownCollection(col);

        assert col0 != null;

        for (Object obj : col)
            col0.add(unwrapBinary(ctx, obj, keepBinary, cpy));

        return col0;
    }

    /**
     * Unwraps map.
     *
     * @param map Map to unwrap.
     * @param keepBinary Keep binary flag.
     * @return Unwrapped collection.
     */
    private static Map<Object, Object> unwrapBinariesIfNeeded(CacheObjectValueContext ctx, Map<Object, Object> map,
        boolean keepBinary, boolean cpy) {
        if (keepBinary)
            return map;

        Map<Object, Object> map0 = BinaryUtils.newMap(map);

        for (Map.Entry<Object, Object> e : map.entrySet())
            map0.put(unwrapBinary(ctx, e.getKey(), false, cpy), unwrapBinary(ctx, e.getValue(), false, cpy));

        return map0;
    }

    /**
     * @param col Collection to unwrap.
     * @param keepBinary Keep binary flag.
     * @param cpy Copy value flag.
     * @return Unwrapped collection.
     */
    private static Collection<Object> unwrapBinariesIfNeeded(CacheObjectValueContext ctx, Collection<Object> col,
        boolean keepBinary, boolean cpy) {
        Collection<Object> col0 = BinaryUtils.newKnownCollection(col);

        if (col0 == null)
            col0 = new ArrayList<>(col.size());

        for (Object obj : col)
            col0.add(unwrapBinary(ctx, obj, keepBinary, cpy));

        return col0;
    }

    /**
     * Unwrap array of binaries if needed.
     *
     * @param arr Array.
     * @param keepBinary Keep binary flag.
     * @param cpy Copy.
     * @return Result.
     */
    private static Object[] unwrapBinariesInArrayIfNeeded(CacheObjectValueContext ctx, Object[] arr, boolean keepBinary,
        boolean cpy) {
        if (BinaryUtils.knownArray(arr))
            return arr;

        Object[] res = new Object[arr.length];

        for (int i = 0; i < arr.length; i++)
            res[i] = unwrapBinary(ctx, arr[i], keepBinary, cpy);

        return res;
    }

    /**
     * @param o Object to unwrap.
     * @return Unwrapped object.
     */
    @SuppressWarnings("unchecked")
    private static Object unwrapBinary(CacheObjectValueContext ctx, Object o, boolean keepBinary, boolean cpy) {
        if (o instanceof Map.Entry) {
            Map.Entry entry = (Map.Entry)o;

            Object key = entry.getKey();

            Object uKey = unwrapBinary(ctx, key, keepBinary, cpy);

            Object val = entry.getValue();

            Object uVal = unwrapBinary(ctx, val, keepBinary, cpy);

            return (key != uKey || val != uVal) ? F.t(uKey, uVal) : o;
        }
        else if (BinaryUtils.knownCollection(o))
            return unwrapKnownCollection(ctx, (Collection<Object>)o, keepBinary, cpy);
        else if (BinaryUtils.knownMap(o))
            return unwrapBinariesIfNeeded(ctx, (Map<Object, Object>)o, keepBinary, cpy);
        else if (o instanceof Object[])
            return unwrapBinariesInArrayIfNeeded(ctx, (Object[])o, keepBinary, cpy);
        else if (o instanceof CacheObject) {
            CacheObject co = (CacheObject)o;

            if (!keepBinary || co.isPlatformType())
                return unwrapBinary(ctx, co.value(ctx, cpy), keepBinary, cpy);
        }

        return o;
    }

    /**
     * Private constructor.
     */
    private CacheObjectUtils() {
        // No-op.
    }
}
