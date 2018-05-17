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

import org.apache.ignite.internal.util.offheap.unsafe.GridUnsafeMemory;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * Compares fiels in serialized form when possible.
 */
public class BinarySerializedFieldComparator {
    /** Position: not found. */
    private static final int POS_NOT_FOUND = -1;

    /** Original object. */
    private final BinaryObjectExImpl obj;

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

    /** Current field order. */
    private int curFieldOrder;

    /** Current field offset. */
    private int curFieldPos;

    /**
     * Constructor.
     *
     * @param obj Original object.
     * @param arr Array.
     * @param ptr Pointer.
     * @param startOff Start offset.
     * @param orderBase Order base.
     * @param orderMultiplier Order multiplier.
     * @param fieldOffLen Field offset length.
     */
    public BinarySerializedFieldComparator(BinaryObjectExImpl obj, byte[] arr, long ptr, int startOff, int orderBase,
        int orderMultiplier, int fieldOffLen) {
        assert arr != null && ptr == 0L || arr == null && ptr != 0L;

        this.obj = obj;
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
        curFieldOrder = order;

        if (order == BinarySchema.ORDER_NOT_FOUND)
            curFieldPos = POS_NOT_FOUND;
        else {
            int pos = orderBase + order * orderMultiplier;

            if (fieldOffLen == BinaryUtils.OFFSET_1) {
                byte val = offheap() ? BinaryPrimitives.readByte(ptr, pos) : BinaryPrimitives.readByte(arr, pos);

                curFieldPos = startOff + ((int)val & 0xFF);
            }
            else if (fieldOffLen == BinaryUtils.OFFSET_2) {
                short val = offheap() ? BinaryPrimitives.readShort(ptr, pos) : BinaryPrimitives.readShort(arr, pos);

                curFieldPos = startOff + ((int)val & 0xFFFF);
            }
            else {
                int val = offheap() ? BinaryPrimitives.readInt(ptr, pos) : BinaryPrimitives.readInt(arr, pos);

                curFieldPos = startOff + val;
            }
        }
    }

    /**
     * Get field type.
     *
     * @return Field type.
     */
    private byte fieldType() {
        if (curFieldPos == POS_NOT_FOUND)
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
     * Get current field.
     *
     * @return Current field.
     */
    private Object currentField() {
        return obj.fieldByOrder(curFieldOrder);
    }

    /**
     * Read byte value.
     *
     * @param off Offset.
     * @return Value.
     */
    private byte readByte(int off) {
        if (offheap())
            return BinaryPrimitives.readByte(ptr, curFieldPos + off);
        else
            return arr[curFieldPos + off];
    }

    /**
     * Read short value.
     *
     * @param off Offset.
     * @return Value.
     */
    private short readShort(int off) {
        if (offheap())
            return BinaryPrimitives.readShort(ptr, curFieldPos + off);
        else
            return BinaryPrimitives.readShort(arr, curFieldPos + off);
    }

    /**
     * Read int value.
     *
     * @param off Offset.
     * @return Value.
     */
    private int readInt(int off) {
        if (offheap())
            return BinaryPrimitives.readInt(ptr, curFieldPos + off);
        else
            return BinaryPrimitives.readInt(arr, curFieldPos + off);
    }

    /**
     * Reads value of length of an array, which can be presented in default format or varint encoding.
     *
     * <a href="https://developers.google.com/protocol-buffers/docs/encoding#varints">Varint encoding description.</a>
     *
     * If you need to know number of bytes which were used for storage of the read value,
     * use the method {@link BinaryUtils#sizeOfArrayLengthValue(int, boolean)}.
     *
     * @param off Offset.
     * @param varint Whether to read arrays lengths in varint encoding.
     * @return Value of array's length.
     */
    private int readArrayLength(int off, boolean varint) {
        if (offheap())
            return BinaryUtils.doReadArrayLength(ptr, curFieldPos + off, varint);
        else
            return BinaryUtils.doReadArrayLength(arr, curFieldPos + off, varint);
    }

    /**
     * Read long value.
     *
     * @param off Offset.
     * @return Value.
     */
    private long readLong(int off) {
        if (offheap())
            return BinaryPrimitives.readLong(ptr, curFieldPos + off);
        else
            return BinaryPrimitives.readLong(arr, curFieldPos + off);
    }

    /**
     * Compare fields.
     *
     * @param c1 First comparer.
     * @param c2 Second comparer.
     * @return {@code True} if both fields are equal.
     */
    public static boolean equals(BinarySerializedFieldComparator c1, BinarySerializedFieldComparator c2) {
        // Compare field types.
        byte typ = c1.fieldType();

        if (typ != c2.fieldType())
            return false;

        // Switch by type and compare.
        switch (typ) {
            case GridBinaryMarshaller.BYTE:
            case GridBinaryMarshaller.BOOLEAN:
                return c1.readByte(1) == c2.readByte(1);

            case GridBinaryMarshaller.SHORT:
            case GridBinaryMarshaller.CHAR:
                return c1.readShort(1) == c2.readShort(1);

            case GridBinaryMarshaller.INT:
            case GridBinaryMarshaller.FLOAT:
                return c1.readInt(1) == c2.readInt(1);

            case GridBinaryMarshaller.LONG:
            case GridBinaryMarshaller.DOUBLE:
            case GridBinaryMarshaller.DATE:
                return c1.readLong(1) == c2.readLong(1);

            case GridBinaryMarshaller.TIMESTAMP:
                return c1.readLong(1) == c2.readLong(1) && c1.readInt(1 + 8) == c2.readInt(1 + 8);

            case GridBinaryMarshaller.TIME:
                return c1.readLong(1) == c2.readLong(1);

            case GridBinaryMarshaller.UUID:
                return c1.readLong(1) == c2.readLong(1) && c1.readLong(1 + 8) == c2.readLong(1 + 8);

            case GridBinaryMarshaller.STRING:
                return compareByteArrays(c1, c2, 1);

            case GridBinaryMarshaller.DECIMAL:
                return c1.readInt(1) == c2.readInt(1) && compareByteArrays(c1, c2, 5);

            case GridBinaryMarshaller.NULL:
                return true;

            default:
                Object val1 = c1.currentField();
                Object val2 = c2.currentField();

                return isArray(val1) ? compareArrays(val1, val2) : F.eq(val1, val2);
        }
    }

    /**
     * Compare arrays.
     *
     * @param val1 Value 1.
     * @param val2 Value 2.
     * @return Result.
     */
    private static boolean compareArrays(Object val1, Object val2) {
        if (val1.getClass() == val2.getClass()) {
            if (val1 instanceof byte[])
                return Arrays.equals((byte[])val1, (byte[])val2);
            else if (val1 instanceof boolean[])
                return Arrays.equals((boolean[])val1, (boolean[])val2);
            else if (val1 instanceof short[])
                return Arrays.equals((short[])val1, (short[])val2);
            else if (val1 instanceof char[])
                return Arrays.equals((char[])val1, (char[])val2);
            else if (val1 instanceof int[])
                return Arrays.equals((int[])val1, (int[])val2);
            else if (val1 instanceof long[])
                return Arrays.equals((long[])val1, (long[])val2);
            else if (val1 instanceof float[])
                return Arrays.equals((float[])val1, (float[])val2);
            else if (val1 instanceof double[])
                return Arrays.equals((double[])val1, (double[])val2);
            else
                return Arrays.deepEquals((Object[])val1, (Object[])val2);
        }

        return false;
    }

    /**
     * @param field Field.
     * @return {@code True} if field is array.
     */
    private static boolean isArray(@Nullable Object field) {
        return field != null && field.getClass().isArray();
    }

    /**
     * Compare byte arrays.
     *
     * @param c1 Comparer 1.
     * @param c2 Comparer 2.
     * @param off Offset (where length is located).
     * @return {@code True} if equal.
     */
    private static boolean compareByteArrays(BinarySerializedFieldComparator c1, BinarySerializedFieldComparator c2,
                                             int off) {
        int len = c1.readArrayLength(off, c1.obj.context().isUseVarintArrayLength());

        if (len != c2.readArrayLength(off, c2.obj.context().isUseVarintArrayLength()))
            return false;
        else {
            off += BinaryUtils.sizeOfArrayLengthValue(len, c1.obj.context().isUseVarintArrayLength());

            if (c1.offheap()) {
                if (c2.offheap())
                    // Case 1: both offheap.
                    return GridUnsafeMemory.compare(c1.curFieldPos + c1.ptr + off, c2.curFieldPos + c2.ptr + off, len);
            }
            else {
                if (!c2.offheap()) {
                    // Case 2: both onheap.
                    for (int i = 0; i < len; i++) {
                        if (c1.arr[c1.curFieldPos + off + i] != c2.arr[c2.curFieldPos + off + i])
                            return false;
                    }

                    return true;
                }
                else {
                    // Swap.
                    BinarySerializedFieldComparator tmp = c1;
                    c1 = c2;
                    c2 = tmp;
                }
            }

            // Case 3: offheap vs onheap.
            assert c1.offheap() && !c2.offheap();

            for (int i = 0; i < len; i++) {
                if (BinaryPrimitives.readByte(c1.ptr, c1.curFieldPos + off + i) != c2.arr[c2.curFieldPos + off + i])
                    return false;
            }

            return true;
        }
    }
}
