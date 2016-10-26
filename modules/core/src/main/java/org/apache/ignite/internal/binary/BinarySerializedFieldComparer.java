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

package org.apache.ignite.internal.binary;

import org.apache.ignite.internal.binary.streams.BinaryHeapInputStream;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Compares fiels in serialized form when possible.
 */
public class BinarySerializedFieldComparer {
    /** Context. */
    private final BinaryContext ctx;

    /** Pointer to data (onheap). */
    private final byte[] arr;

    /** Pointer to data (offheap). */
    private final long ptr;

    /** Object start offset. */
    private final int startOff;

    /** Order base. */
    private final int orderBase;

    /** Order multiplier. */
    private final int orderMultiplier;

    /** Field offset length. */
    private final int fieldOffLen;

    /** Current field offset. */
    private int curFieldPos;

    /**
     * Constructor.
     *
     * @param arr Array.
     * @param ptr Pointer.
     * @param startOff Start offset.
     * @param orderBase Order base.
     * @param orderMultiplier Order multiplier.
     * @param fieldOffLen Field offset length.
     */
    public BinarySerializedFieldComparer(BinaryContext ctx, byte[] arr, long ptr, int startOff, int orderBase,
        int orderMultiplier, int fieldOffLen) {
        assert arr != null && ptr == 0L || arr == null && ptr != 0L;

        this.ctx = ctx;
        this.arr = arr;
        this.ptr = ptr;
        this.startOff = startOff;
        this.orderBase = orderBase;
        this.orderMultiplier = orderMultiplier;
        this.fieldOffLen = fieldOffLen;
    }

    /**
     * Locate the field.
     *
     * @param order Field order.
     */
    public void findField(int order) {
        if (order == BinarySchema.ORDER_NOT_FOUND)
            curFieldPos = -1;

        int curFieldOffPos = orderBase + order * orderMultiplier;

        if (fieldOffLen == BinaryUtils.OFFSET_1)
            curFieldPos = startOff + ((int)BinaryPrimitives.readByte(arr, curFieldOffPos) & 0xFF);
        else if (fieldOffLen == BinaryUtils.OFFSET_2)
            curFieldPos = startOff + ((int)BinaryPrimitives.readShort(arr, curFieldOffPos) & 0xFFFF);
        else
            curFieldPos = startOff + BinaryPrimitives.readInt(arr, curFieldOffPos);
    }

    /**
     * Get field type.
     *
     * @return Field type.
     */
    private byte fieldType() {
        if (curFieldPos == -1)
            return GridBinaryMarshaller.NULL;
        else
            return offheap() ?
                BinaryPrimitives.readByte(ptr, curFieldPos) : BinaryPrimitives.readByte(arr, curFieldPos);
    }

    /**
     * @return Whether this is offheap object.
     */
    private boolean offheap() {
        return ptr != 0L;
    }

    /**
     * Compare fields.
     *
     * @param c1 First comparer.
     * @param c2 Second comparer.
     * @return {@code True} if both fields are equal.
     */
    public static boolean equals(BinarySerializedFieldComparer c1, BinarySerializedFieldComparer c2) {
        assert c1.offheap() && c2.offheap() || !c1.offheap() && !c2.offheap();

        if (c1.offheap())
            return equalsOffheap(c1, c2);
        else
            return equalsOnheap(c1, c2);
    }

    /**
     * Compare fields of onheap objects.
     *
     * @param c1 First comparer.
     * @param c2 Second comparer.
     * @return {@code True} if both fields are equal.
     */
    private static boolean equalsOnheap(BinarySerializedFieldComparer c1, BinarySerializedFieldComparer c2) {
        // Compare field types.
        byte typ1 = c1.fieldType();
        byte typ2 = c2.fieldType();

        if (typ1 != typ2)
            return false;

        // Switch by type and compare.
        switch (typ1) {
            case GridBinaryMarshaller.BYTE:
            case GridBinaryMarshaller.BOOLEAN: {
                byte val1 = GridUnsafe.getByte(c1.arr, c1.curFieldPos + 1);
                byte val2 = GridUnsafe.getByte(c2.arr, c2.curFieldPos + 1);

                return val1 == val2;
            }

            case GridBinaryMarshaller.SHORT:
            case GridBinaryMarshaller.CHAR: {
                short val1 = BinaryPrimitives.readShort(c1.arr, c1.curFieldPos + 1);
                short val2 = BinaryPrimitives.readShort(c2.arr, c2.curFieldPos + 1);

                return val1 == val2;
            }

            case GridBinaryMarshaller.INT:
            case GridBinaryMarshaller.FLOAT: {
                int val1 = BinaryPrimitives.readInt(c1.arr, c1.curFieldPos + 1);
                int val2 = BinaryPrimitives.readInt(c2.arr, c2.curFieldPos + 1);

                return val1 == val2;
            }

            case GridBinaryMarshaller.LONG:
            case GridBinaryMarshaller.DOUBLE:
            case GridBinaryMarshaller.DATE: {
                long val1 = BinaryPrimitives.readLong(c1.arr, c1.curFieldPos + 1);
                long val2 = BinaryPrimitives.readLong(c2.arr, c2.curFieldPos + 1);

                return val1 == val2;
            }

            case GridBinaryMarshaller.TIMESTAMP: {
                long longVal1 = BinaryPrimitives.readLong(c1.arr, c1.curFieldPos + 1);
                long longVal2 = BinaryPrimitives.readLong(c2.arr, c2.curFieldPos + 1);

                if (longVal1 == longVal2) {
                    int intVal1 = BinaryPrimitives.readInt(c1.arr, c1.curFieldPos + 1 + 8);
                    int intVal2 = BinaryPrimitives.readInt(c2.arr, c2.curFieldPos + 1 + 8);

                    return intVal1 == intVal2;
                }

                return false;
            }

            case GridBinaryMarshaller.UUID: {
                long longVal1 = BinaryPrimitives.readLong(c1.arr, c1.curFieldPos + 1);
                long longVal2 = BinaryPrimitives.readLong(c2.arr, c2.curFieldPos + 1);

                if (longVal1 == longVal2) {
                    longVal1 = BinaryPrimitives.readLong(c1.arr, c1.curFieldPos + 1 + 8);
                    longVal2 = BinaryPrimitives.readLong(c2.arr, c2.curFieldPos + 1 + 8);

                    return longVal1 == longVal2;
                }

                return false;
            }

            case GridBinaryMarshaller.STRING: {
                int len1 = BinaryPrimitives.readInt(c1.arr, c1.curFieldPos + 1);
                int len2 = BinaryPrimitives.readInt(c2.arr, c2.curFieldPos + 1);

                return len1 == len2 && compareData(c1.arr, c1.curFieldPos + 5, c2.arr, c2.curFieldPos + 5, len1);
            }

            case GridBinaryMarshaller.DECIMAL: {
                int scale1 = BinaryPrimitives.readInt(c1.arr, c1.curFieldPos + 1);
                int scale2 = BinaryPrimitives.readInt(c2.arr, c2.curFieldPos + 1);

                if (scale1 == scale2) {
                    int len1 = BinaryPrimitives.readInt(c1.arr, c1.curFieldPos + 5);
                    int len2 = BinaryPrimitives.readInt(c2.arr, c2.curFieldPos + 5);

                    return len1 == len2 && compareData(c1.arr, c1.curFieldPos + 9, c2.arr, c2.curFieldPos + 9, len1);
                }

                return false;
            }

            case GridBinaryMarshaller.NULL:
                return true;

            default: {
                Object val1 = BinaryUtils.unmarshal(BinaryHeapInputStream.create(c1.arr, c1.curFieldPos), c1.ctx, null);
                Object val2 = BinaryUtils.unmarshal(BinaryHeapInputStream.create(c2.arr, c2.curFieldPos), c2.ctx, null);

                return F.eq(val1, val2);
            }
        }
    }

    /**
     * Compare data in two arrays.
     *
     * @param a1 Array 1.
     * @param off1 Offset 1.
     * @param a2 Array 2.
     * @param off2 Offset 2.
     * @param len Length.
     * @return {@code True} if equal.
     */
    private static boolean compareData(byte[] a1, int off1, byte[] a2, int off2, int len) {
        for (int i = 0; i < len; i++) {
            if (a1[off1 + i] != a2[off2 + i])
                return false;
        }

        return true;
    }

    /**
     * Compare fields of offheap objects.
     *
     * @param c1 First comparer.
     * @param c2 Second comparer.
     * @return {@code True} if both fields are equal.
     */
    private static boolean equalsOffheap(BinarySerializedFieldComparer c1, BinarySerializedFieldComparer c2) {
        // TODO: Implement
        throw new UnsupportedOperationException("Should not be called!");
    }
}
