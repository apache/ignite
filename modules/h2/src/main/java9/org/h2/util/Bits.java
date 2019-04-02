/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.util;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.UUID;

/**
 * Manipulations with bytes and arrays. Specialized implementation for Java 9
 * and later versions.
 */
public final class Bits {

    /**
     * VarHandle giving access to elements of a byte[] array viewed as if it
     * were a int[] array on big-endian system.
     */
    private static final VarHandle INT_VH_BE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.BIG_ENDIAN);

    /**
     * VarHandle giving access to elements of a byte[] array viewed as if it
     * were a int[] array on little-endian system.
     */
    private static final VarHandle INT_VH_LE = MethodHandles.byteArrayViewVarHandle(int[].class,
            ByteOrder.LITTLE_ENDIAN);

    /**
     * VarHandle giving access to elements of a byte[] array viewed as if it
     * were a long[] array on big-endian system.
     */
    private static final VarHandle LONG_VH_BE = MethodHandles.byteArrayViewVarHandle(long[].class,
            ByteOrder.BIG_ENDIAN);

    /**
     * VarHandle giving access to elements of a byte[] array viewed as if it
     * were a long[] array on little-endian system.
     */
    private static final VarHandle LONG_VH_LE = MethodHandles.byteArrayViewVarHandle(long[].class,
            ByteOrder.LITTLE_ENDIAN);

    /**
     * VarHandle giving access to elements of a byte[] array viewed as if it
     * were a double[] array on big-endian system.
     */
    private static final VarHandle DOUBLE_VH_BE = MethodHandles.byteArrayViewVarHandle(double[].class,
            ByteOrder.BIG_ENDIAN);

    /**
     * VarHandle giving access to elements of a byte[] array viewed as if it
     * were a double[] array on little-endian system.
     */
    private static final VarHandle DOUBLE_VH_LE = MethodHandles.byteArrayViewVarHandle(double[].class,
            ByteOrder.LITTLE_ENDIAN);

    /**
     * Compare the contents of two char arrays. If the content or length of the
     * first array is smaller than the second array, -1 is returned. If the content
     * or length of the second array is smaller than the first array, 1 is returned.
     * If the contents and lengths are the same, 0 is returned.
     *
     * @param data1
     *            the first char array (must not be null)
     * @param data2
     *            the second char array (must not be null)
     * @return the result of the comparison (-1, 1 or 0)
     */
    public static int compareNotNull(char[] data1, char[] data2) {
        return Integer.signum(Arrays.compare(data1, data2));
    }

    /**
     * Compare the contents of two byte arrays. If the content or length of the
     * first array is smaller than the second array, -1 is returned. If the content
     * or length of the second array is smaller than the first array, 1 is returned.
     * If the contents and lengths are the same, 0 is returned.
     *
     * <p>
     * This method interprets bytes as signed.
     * </p>
     *
     * @param data1
     *            the first byte array (must not be null)
     * @param data2
     *            the second byte array (must not be null)
     * @return the result of the comparison (-1, 1 or 0)
     */
    public static int compareNotNullSigned(byte[] data1, byte[] data2) {
        return Integer.signum(Arrays.compare(data1, data2));
    }

    /**
     * Compare the contents of two byte arrays. If the content or length of the
     * first array is smaller than the second array, -1 is returned. If the content
     * or length of the second array is smaller than the first array, 1 is returned.
     * If the contents and lengths are the same, 0 is returned.
     *
     * <p>
     * This method interprets bytes as unsigned.
     * </p>
     *
     * @param data1
     *            the first byte array (must not be null)
     * @param data2
     *            the second byte array (must not be null)
     * @return the result of the comparison (-1, 1 or 0)
     */
    public static int compareNotNullUnsigned(byte[] data1, byte[] data2) {
        return Integer.signum(Arrays.compareUnsigned(data1, data2));
    }

    /**
     * Reads a int value from the byte array at the given position in big-endian
     * order.
     *
     * @param buff
     *            the byte array
     * @param pos
     *            the position
     * @return the value
     */
    public static int readInt(byte[] buff, int pos) {
        return (int) INT_VH_BE.get(buff, pos);
    }

    /**
     * Reads a int value from the byte array at the given position in
     * little-endian order.
     *
     * @param buff
     *            the byte array
     * @param pos
     *            the position
     * @return the value
     */
    public static int readIntLE(byte[] buff, int pos) {
        return (int) INT_VH_LE.get(buff, pos);
    }

    /**
     * Reads a long value from the byte array at the given position in
     * big-endian order.
     *
     * @param buff
     *            the byte array
     * @param pos
     *            the position
     * @return the value
     */
    public static long readLong(byte[] buff, int pos) {
        return (long) LONG_VH_BE.get(buff, pos);
    }

    /**
     * Reads a long value from the byte array at the given position in
     * little-endian order.
     *
     * @param buff
     *            the byte array
     * @param pos
     *            the position
     * @return the value
     */
    public static long readLongLE(byte[] buff, int pos) {
        return (long) LONG_VH_LE.get(buff, pos);
    }

    /**
     * Reads a double value from the byte array at the given position in
     * big-endian order.
     *
     * @param buff
     *            the byte array
     * @param pos
     *            the position
     * @return the value
     */
    public static double readDouble(byte[] buff, int pos) {
        return (double) DOUBLE_VH_BE.get(buff, pos);
    }

    /**
     * Reads a double value from the byte array at the given position in
     * little-endian order.
     *
     * @param buff
     *            the byte array
     * @param pos
     *            the position
     * @return the value
     */
    public static double readDoubleLE(byte[] buff, int pos) {
        return (double) DOUBLE_VH_LE.get(buff, pos);
    }

    /**
     * Converts UUID value to byte array in big-endian order.
     *
     * @param msb
     *            most significant part of UUID
     * @param lsb
     *            least significant part of UUID
     * @return byte array representation
     */
    public static byte[] uuidToBytes(long msb, long lsb) {
        byte[] buff = new byte[16];
        LONG_VH_BE.set(buff, 0, msb);
        LONG_VH_BE.set(buff, 8, lsb);
        return buff;
    }

    /**
     * Converts UUID value to byte array in big-endian order.
     *
     * @param uuid
     *            UUID value
     * @return byte array representation
     */
    public static byte[] uuidToBytes(UUID uuid) {
        return uuidToBytes(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
    }

    /**
     * Writes a int value to the byte array at the given position in big-endian
     * order.
     *
     * @param buff
     *            the byte array
     * @param pos
     *            the position
     * @param x
     *            the value to write
     */
    public static void writeInt(byte[] buff, int pos, int x) {
        INT_VH_BE.set(buff, pos, x);
    }

    /**
     * Writes a int value to the byte array at the given position in
     * little-endian order.
     *
     * @param buff
     *            the byte array
     * @param pos
     *            the position
     * @param x
     *            the value to write
     */
    public static void writeIntLE(byte[] buff, int pos, int x) {
        INT_VH_LE.set(buff, pos, x);
    }

    /**
     * Writes a long value to the byte array at the given position in big-endian
     * order.
     *
     * @param buff
     *            the byte array
     * @param pos
     *            the position
     * @param x
     *            the value to write
     */
    public static void writeLong(byte[] buff, int pos, long x) {
        LONG_VH_BE.set(buff, pos, x);
    }

    /**
     * Writes a long value to the byte array at the given position in
     * little-endian order.
     *
     * @param buff
     *            the byte array
     * @param pos
     *            the position
     * @param x
     *            the value to write
     */
    public static void writeLongLE(byte[] buff, int pos, long x) {
        LONG_VH_LE.set(buff, pos, x);
    }

    /**
     * Writes a double value to the byte array at the given position in
     * big-endian order.
     *
     * @param buff
     *            the byte array
     * @param pos
     *            the position
     * @param x
     *            the value to write
     */
    public static void writeDouble(byte[] buff, int pos, double x) {
        DOUBLE_VH_BE.set(buff, pos, x);
    }

    /**
     * Writes a double value to the byte array at the given position in
     * little-endian order.
     *
     * @param buff
     *            the byte array
     * @param pos
     *            the position
     * @param x
     *            the value to write
     */
    public static void writeDoubleLE(byte[] buff, int pos, double x) {
        DOUBLE_VH_LE.set(buff, pos, x);
    }

    private Bits() {
    }
}
