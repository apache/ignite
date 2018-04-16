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
import java.util.Comparator;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.ignite.binary.BinaryInvalidTypeException;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.util.MutableSingletonList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

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
    public static Object unwrapBinaryIfNeeded(CacheObjectValueContext ctx, CacheObject o, boolean keepBinary, boolean cpy) {
        return unwrapBinary(ctx, o, keepBinary, cpy);
    }

    /**
     * @param o Object to unwrap.
     * @param keepBinary Keep binary flag.
     * @param cpy Copy value flag.
     * @return Unwrapped object.
     */
    public static Object unwrapBinaryIfNeeded(CacheObjectValueContext ctx, Object o, boolean keepBinary, boolean cpy) {
        if (o == null)
            return null;

        // TODO has to be overloaded
        if (o instanceof Map.Entry) {
            Map.Entry entry = (Map.Entry)o;

            Object key = entry.getKey();

            Object uKey = unwrapBinary(ctx, key, keepBinary, cpy);

            Object val = entry.getValue();

            Object uVal = unwrapBinary(ctx, val, keepBinary, cpy);

            return (key != uKey || val != uVal) ? F.t(uKey, uVal) : o;
        }

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

        return (col0 instanceof MutableSingletonList) ? U.convertToSingletonList(col0) : col0;
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
            // TODO why don't we use keepBinary parameter here?
            map0.put(
                unwrapBinary(ctx, e.getKey(), false, cpy),
                unwrapBinary(ctx, e.getValue(), false, cpy));

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
            col0.add(unwrapBinaryIfNeeded(ctx, obj, keepBinary, cpy));

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
        if (o == null)
            return o;

        while (BinaryUtils.knownCacheObject(o)) {
            CacheObject co = (CacheObject)o;

            if (!co.isPlatformType() && keepBinary)
                return o;

            // It may be a collection of binaries
            o = co.value(ctx, cpy);
        }

        if (BinaryUtils.knownCollection(o))
            return unwrapKnownCollection(ctx, (Collection<Object>)o, keepBinary, cpy);
        else if (BinaryUtils.knownMap(o))
            return unwrapBinariesIfNeeded(ctx, (Map<Object, Object>)o, keepBinary, cpy);
        else if (o instanceof Object[])
            return unwrapBinariesInArrayIfNeeded(ctx, (Object[])o, keepBinary, cpy);

        return o;
    }

    /**
     * Private constructor.
     */
    private CacheObjectUtils() {
        // No-op.
    }

    /**
     * @param col Collection to sort.
     * @param ctx Cache context to get value if keys in given map are CacheKeyObjects.
     * @return {@code null} if given collection was null, same {@code col} if it contains only one entry or
     * it is already sorted (and given comparator same as current),
     *  and {@code SortedSet} if it was successfully sorted.
     */
    public static <T> Collection<T> sort(@Nullable Collection<T> col, CacheObjectValueContext ctx) {
        if (col == null || col.size() == 1 || col instanceof SortedSet)
            return col;

        SortedSet<T> sortedSet = new TreeSet<>(compatibleComparator(col, ctx));

        sortedSet.addAll(col);

        return sortedSet;
    }

    /**
     * @param map Map to sort.
     * @param ctx Cache context to get value if keys in given map are CacheKeyObjects.
     * @return {@code null} if given map was null, same {@code map} if it contains only one entry or
     * it is already sorted, {@code new SortedMap} if it was successfully sorted.
     */
    public static <K, V> Map<K, V> sort(@Nullable Map<K, V> map, CacheObjectValueContext ctx) {
        if (map == null || map.size() == 1 || map instanceof SortedMap)
            return map;

        SortedMap<K, V> sortedMap = new TreeMap<>(compatibleComparator(map.keySet(), ctx));

        sortedMap.putAll(map);

        return sortedMap;
    }

    /**
     * @return {@link KeyCacheObjectComparator KeyCacheObjectComparator}
     * if all given objects are {@link KeyCacheObject KeyCacheObjects}.
     * {@link HashcodeComparator} for other objects.
     */
    private static <T> Comparator<T> compatibleComparator(Collection<T> col, CacheObjectValueContext ctx) {
        for (T obj : col) {
            if (!(obj instanceof KeyCacheObject))
                return new HashcodeComparator<T>();
        }

        return (Comparator<T>) new KeyCacheObjectComparator(ctx);
    }

    /**
     * Compare values inside KeyCacheObjects.
     */
    private static final class KeyCacheObjectComparator implements Comparator<KeyCacheObject> {
        /** */
        private final CacheObjectValueContext ctx;

        /**
         * @param ctx Context.
         */
        private KeyCacheObjectComparator(CacheObjectValueContext ctx) {
            this.ctx = ctx;
        }

        /** {@inheritDoc} */
        @Override public int compare(KeyCacheObject o1, KeyCacheObject o2) {
            Object val1 = getValueFromKeyCacheObject(o1, ctx);
            Object val2 = getValueFromKeyCacheObject(o2, ctx);

            return compareObjects(val1, val2);
        }

        /**
         * @param key Where value is stored.
         * @param ctx Context to retrieve the value from key.
         * @return Value of the key.
         */
        private Object getValueFromKeyCacheObject(KeyCacheObject key, CacheObjectValueContext ctx) {
            try {
                return key.value(ctx, false);
            } catch (BinaryInvalidTypeException e) {
                return key.hashCode();
            }
        }
    }

    /**
     * Compares objects by {@link Comparable#compareTo(Object) compareTo} method of possible.
     * If objects are not comparable, compares them by hashcode.
     */
    private static final class HashcodeComparator<T> implements Comparator<T> {
        /** {@inheritDoc} */
        @Override public int compare(Object o1, Object o2) {
            return compareObjects(o1, o2);
        }
    }

    /**
     * @param o1 Object to be compared.
     * @param o2 Object to be compared.
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     */
    private static int compareObjects(Object o1, Object o2) {
        if (o1 == null && o2 != null)
            return -1;

        if (o1 == null)
            return 0;

        if (o2 == null)
            return 1;

        if (o1 instanceof Comparable && o1.getClass().equals(o2.getClass()))
            return ((Comparable)o1).compareTo(o2);

        return Integer.compare(o1.hashCode(), o2.hashCode());
    }
}
