/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.util.MutableSingletonList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;

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
}
