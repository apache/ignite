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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.binary.BinaryUtils;
import org.apache.ignite.internal.pagemem.PageUtils;
import org.apache.ignite.internal.util.CommonUtils;
import org.apache.ignite.internal.util.MutableSingletonList;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

/**
 * Cache object utility methods.
 */
public class CacheObjectUtils {
    /** Head size. */
    protected static final int HEAD_SIZE = 5; // 4 bytes len + 1 byte type

    /**
     * @param o Object to unwrap.
     * @param keepBinary Keep binary flag.
     * @param cpy Copy value flag.
     * @return Unwrapped object.
     */
    public static Object unwrapBinaryIfNeeded(CacheObjectValueContext ctx, CacheObject o, boolean keepBinary, boolean cpy) {
        return unwrapBinary(ctx, o, keepBinary, cpy, null);
    }

    /**
     * @param ctx Cache object context.
     * @param o Object to unwrap.
     * @param keepBinary Keep binary flag.
     * @param cpy Copy value flag.
     * @param ldr Class loader, used for deserialization from binary representation.
     * @return Unwrapped object.
     */
    public static Object unwrapBinaryIfNeeded(
        CacheObjectValueContext ctx,
        Object o,
        boolean keepBinary,
        boolean cpy,
        @Nullable ClassLoader ldr
    ) {
        if (o == null)
            return null;

        // TODO has to be overloaded
        if (o instanceof Map.Entry) {
            Map.Entry entry = (Map.Entry)o;

            Object key = entry.getKey();

            Object uKey = unwrapBinary(ctx, key, keepBinary, cpy, ldr);

            Object val = entry.getValue();

            Object uVal = unwrapBinary(ctx, val, keepBinary, cpy, ldr);

            return (key != uKey || val != uVal) ? F.t(uKey, uVal) : o;
        }

        return unwrapBinary(ctx, o, keepBinary, cpy, ldr);
    }

    /**
     * @param col Collection of objects to unwrap.
     * @param keepBinary Keep binary flag.
     * @return Unwrapped collection.
     */
    public static Collection<Object> unwrapBinariesIfNeeded(CacheObjectValueContext ctx, Collection<?> col,
        boolean keepBinary) {
        return unwrapBinariesIfNeeded(ctx, col, keepBinary, true);
    }

    /**
     * @param col Collection to unwrap.
     * @param keepBinary Keep binary flag.
     * @param cpy Copy flag.
     * @return Unwrapped collection.
     */
    private static Collection<Object> unwrapKnownCollection(CacheObjectValueContext ctx, Collection<?> col,
        boolean keepBinary, boolean cpy) {
        Collection<Object> col0 = BinaryUtils.newKnownCollection(col);

        assert col0 != null;

        for (Object obj : col)
            col0.add(unwrapBinary(ctx, obj, keepBinary, cpy, null));

        return (col0 instanceof MutableSingletonList) ? CommonUtils.convertToSingletonList(col0) : col0;
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
                unwrapBinary(ctx, e.getKey(), false, cpy, null),
                unwrapBinary(ctx, e.getValue(), false, cpy, null));

        return map0;
    }

    /**
     * @param col Collection to unwrap.
     * @param keepBinary Keep binary flag.
     * @param cpy Copy value flag.
     * @return Unwrapped collection.
     */
    private static Collection<Object> unwrapBinariesIfNeeded(CacheObjectValueContext ctx, Collection<?> col,
        boolean keepBinary, boolean cpy) {
        Collection<Object> col0 = BinaryUtils.newKnownCollection(col);

        if (col0 == null)
            col0 = new ArrayList<>(col.size());

        for (Object obj : col)
            col0.add(unwrapBinaryIfNeeded(ctx, obj, keepBinary, cpy, null));

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
            res[i] = unwrapBinary(ctx, arr[i], keepBinary, cpy, null);

        return res;
    }

    /**
     * Unwraps an object for end user.
     *
     * @param ctx Cache object context.
     * @param o Object to unwrap.
     * @param keepBinary False when need to deserialize object from a binary one, true otherwise.
     * @param cpy True means the object will be copied before return, false otherwise.
     * @param ldr Class loader, used for deserialization from binary representation.
     * @return Unwrapped object.
     */
    public static Object unwrapBinary(
        CacheObjectValueContext ctx,
        Object o,
        boolean keepBinary,
        boolean cpy,
        @Nullable ClassLoader ldr
    ) {
        if (o == null)
            return o;

        while (o instanceof CacheObject) {
            CacheObject co = (CacheObject)o;

            if (!co.isPlatformType() && keepBinary)
                return o;

            // It may be a collection of binaries
            o = co.value(ctx, cpy, ldr);
        }

        if (BinaryUtils.knownCollection(o))
            return unwrapKnownCollection(ctx, (Collection<Object>)o, keepBinary, cpy);
        else if (BinaryUtils.knownMap(o))
            return unwrapBinariesIfNeeded(ctx, (Map<Object, Object>)o, keepBinary, cpy);
        else if (o instanceof Object[] && !BinaryUtils.useBinaryArrays())
            return unwrapBinariesInArrayIfNeeded(ctx, (Object[])o, keepBinary, cpy);
        else if (BinaryUtils.isBinaryArray(o) && !keepBinary)
            return ((BinaryObject)o).deserialize(ldr);

        return o;
    }

    /**
     * @param dataLen Serialized value length.
     * @return Full size required to store cache object.
     * @see #putValue(byte, ByteBuffer, int, int, byte[], int)
     */
    public static int objectPutSize(int dataLen) {
        return dataLen + HEAD_SIZE;
    }

    /**
     * @param addr Write address.
     * @param type Object type.
     * @param valBytes Value bytes array.
     * @return Offset shift compared to initial address.
     */
    public static int putValue(long addr, byte type, byte[] valBytes) {
        return putValue(addr, type, valBytes, 0, valBytes.length);
    }

    /**
     * @param addr Write address.
     * @param type Object type.
     * @param srcBytes Source value bytes array.
     * @param srcOff Start position in sourceBytes.
     * @param len Number of bytes for write.
     * @return Offset shift compared to initial address.
     */
    public static int putValue(long addr, byte type, byte[] srcBytes, int srcOff, int len) {
        int off = 0;

        PageUtils.putInt(addr, off, len);
        off += 4;

        PageUtils.putByte(addr, off, type);
        off++;

        PageUtils.putBytes(addr, off, srcBytes, srcOff, len);
        off += len;

        return off;
    }

    /**
     * @param cacheObjType Cache object type.
     * @param buf Buffer to write value to.
     * @param off Offset in source binary data.
     * @param len Length of the data to write.
     * @param valBytes Binary data.
     * @param start Start offset in binary data.
     * @return {@code True} if data were successfully written.
     * @throws IgniteCheckedException If failed.
     */
    public static boolean putValue(byte cacheObjType,
                                   final ByteBuffer buf,
                                   int off,
                                   int len,
                                   byte[] valBytes,
                                   final int start
    ) throws IgniteCheckedException {
        int dataLen = valBytes.length;

        if (buf.remaining() < len)
            return false;

        if (off == 0 && len >= HEAD_SIZE) {
            buf.putInt(dataLen);
            buf.put(cacheObjType);

            len -= HEAD_SIZE;
        }
        else if (off >= HEAD_SIZE)
            off -= HEAD_SIZE;
        else {
            // Partial header write.
            final ByteBuffer head = ByteBuffer.allocate(HEAD_SIZE);

            head.order(buf.order());

            head.putInt(dataLen);
            head.put(cacheObjType);

            head.position(off);

            if (len < head.capacity())
                head.limit(off + Math.min(len, head.capacity() - off));

            buf.put(head);

            if (head.limit() < HEAD_SIZE)
                return true;

            len -= HEAD_SIZE - off;
            off = 0;
        }

        buf.put(valBytes, start + off, len);

        return true;
    }

    /**
     * Private constructor.
     */
    private CacheObjectUtils() {
        // No-op.
    }
}
