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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.binary.BinaryArray;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.util.MutableSingletonList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.cache.CacheReturnMode.DESERIALIZED;

/**
 * Cache object utility methods.
 */
public class CacheObjectUtils {
    /**
     * @param o Object to unwrap.
     * @param cacheReturnMode Cache return mode.
     * @param cpy Copy value flag.
     * @return Unwrapped object.
     */
    public static Object unwrapBinaryIfNeeded(CacheObjectValueContext ctx, CacheObject o, CacheReturnMode cacheReturnMode, boolean cpy) {
        return unwrapBinary(ctx, o, cacheReturnMode, cpy, null);
    }

    /**
     * @param ctx Cache object context.
     * @param o Object to unwrap.
     * @param cacheReturnMode Cache return mode.
     * @param cpy Copy value flag.
     * @param ldr Class loader, used for deserialization from binary representation.
     * @return Unwrapped object.
     */
    public static Object unwrapBinaryIfNeeded(
        CacheObjectValueContext ctx,
        Object o,
        CacheReturnMode cacheReturnMode,
        boolean cpy,
        @Nullable ClassLoader ldr
    ) {
        if (o == null)
            return null;

        // TODO has to be overloaded
        if (o instanceof Map.Entry) {
            Map.Entry entry = (Map.Entry)o;

            Object key = entry.getKey();

            Object uKey = unwrapBinary(ctx, key, cacheReturnMode, cpy, ldr);

            Object val = entry.getValue();

            Object uVal = unwrapBinary(ctx, val, cacheReturnMode, cpy, ldr);

            return (key != uKey || val != uVal) ? F.t(uKey, uVal) : o;
        }

        return unwrapBinary(ctx, o, cacheReturnMode, cpy, ldr);
    }

    /**
     * @param col Collection of objects to unwrap.
     * @param cacheReturnMode Cache return mode.
     * @return Unwrapped collection.
     */
    public static Collection<Object> unwrapBinariesIfNeeded(
        CacheObjectValueContext ctx,
        Collection<?> col,
        CacheReturnMode cacheReturnMode
    ) {
        return unwrapBinariesIfNeeded(ctx, col, cacheReturnMode, true);
    }

    /**
     * @param col Collection to unwrap.
     * @param cacheReturnMode Cache return mode.
     * @param cpy Copy flag.
     * @return Unwrapped collection.
     */
    private static Collection<Object> unwrapKnownCollection(
        CacheObjectValueContext ctx,
        Collection<?> col,
        CacheReturnMode cacheReturnMode,
        boolean cpy
    ) {
        Collection<Object> col0 = BinaryUtils.newKnownCollection(col);

        assert col0 != null;

        for (Object obj : col)
            col0.add(unwrapBinary(ctx, obj, cacheReturnMode, cpy, null));

        return (col0 instanceof MutableSingletonList) ? U.convertToSingletonList(col0) : col0;
    }

    /**
     * Unwraps map.
     *
     * @param map Map to unwrap.
     * @param cacheReturnMode Cache return mode.
     * @return Unwrapped collection.
     */
    private static Map<Object, Object> unwrapBinariesIfNeeded(
        CacheObjectValueContext ctx,
        Map<Object, Object> map,
        CacheReturnMode cacheReturnMode,
        boolean cpy
    ) {
        assert cacheReturnMode != CacheReturnMode.RAW;

        if (cacheReturnMode == CacheReturnMode.BINARY)
            return map;

        Map<Object, Object> map0 = BinaryUtils.newMap(map);

        for (Map.Entry<Object, Object> e : map.entrySet())
            // TODO why don't we use keepBinary parameter here?
            map0.put(
                unwrapBinary(ctx, e.getKey(), DESERIALIZED, cpy, null),
                unwrapBinary(ctx, e.getValue(), DESERIALIZED, cpy, null));

        return map0;
    }

    /**
     * @param col Collection to unwrap.
     * @param cacheReturnMode Cache return mode.
     * @param cpy Copy value flag.
     * @return Unwrapped collection.
     */
    private static Collection<Object> unwrapBinariesIfNeeded(
        CacheObjectValueContext ctx,
        Collection<?> col,
        CacheReturnMode cacheReturnMode,
        boolean cpy
    ) {
        Collection<Object> col0 = BinaryUtils.newKnownCollection(col);

        if (col0 == null)
            col0 = new ArrayList<>(col.size());

        for (Object obj : col)
            col0.add(unwrapBinaryIfNeeded(ctx, obj, cacheReturnMode, cpy, null));

        return col0;
    }

    /**
     * Unwrap array of binaries if needed.
     *
     * @param arr Array.
     * @param cacheReturnMode Cache return mode.
     * @param cpy Copy.
     * @return Result.
     */
    private static Object[] unwrapBinariesInArrayIfNeeded(
        CacheObjectValueContext ctx,
        Object[] arr,
        CacheReturnMode cacheReturnMode,
        boolean cpy
    ) {
        if (BinaryUtils.knownArray(arr))
            return arr;

        Object[] res = new Object[arr.length];

        for (int i = 0; i < arr.length; i++)
            res[i] = unwrapBinary(ctx, arr[i], cacheReturnMode, cpy, null);

        return res;
    }

    /**
     * Unwraps an object for end user.
     *
     * @param ctx Cache object context.
     * @param o Object to unwrap.
     * @param cacheReturnMode Cache return mode.
     * @param cpy True means the object will be copied before return, false otherwise.
     * @param ldr Class loader, used for deserialization from binary representation.
     * @return Unwrapped object.
     */
    public static Object unwrapBinary(
        CacheObjectValueContext ctx,
        Object o,
        CacheReturnMode cacheReturnMode,
        boolean cpy,
        @Nullable ClassLoader ldr
    ) {
        if (o == null)
            return o;

        if (cacheReturnMode == CacheReturnMode.RAW) {
            assert o instanceof CacheObject;

            return o;
        }

        while (BinaryUtils.knownCacheObject(o)) {
            CacheObject co = (CacheObject)o;

            if (!co.isPlatformType() && cacheReturnMode == CacheReturnMode.BINARY)
                return o;

            // It may be a collection of binaries
            o = co.value(ctx, cpy, ldr);
        }

        if (BinaryUtils.knownCollection(o))
            return unwrapKnownCollection(ctx, (Collection<Object>)o, cacheReturnMode, cpy);
        else if (BinaryUtils.knownMap(o))
            return unwrapBinariesIfNeeded(ctx, (Map<Object, Object>)o, cacheReturnMode, cpy);
        else if (o instanceof Object[] && !BinaryArray.useBinaryArrays())
            return unwrapBinariesInArrayIfNeeded(ctx, (Object[])o, cacheReturnMode, cpy);
        else if (o instanceof BinaryArray && cacheReturnMode == DESERIALIZED)
            return ((BinaryObject)o).deserialize(ldr);

        return o;
    }

    /**
     * Private constructor.
     */
    private CacheObjectUtils() {
        // No-op.
    }
}
