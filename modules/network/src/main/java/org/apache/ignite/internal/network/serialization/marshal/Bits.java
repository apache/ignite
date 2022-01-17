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

package org.apache.ignite.internal.network.serialization.marshal;

/**
 * Code for packing/unpacking primitive values to/from a byte array at specific offsets.
 */
class Bits {
    static boolean getBoolean(byte[] b, int off) {
        return b[off] != 0;
    }

    static char getChar(byte[] b, int off) {
        return (char) ((b[off + 1] & 0xFF)
                + (b[off] << 8));
    }

    static short getShort(byte[] b, int off) {
        return (short) ((b[off + 1] & 0xFF)
                + (b[off] << 8));
    }

    static int getInt(byte[] b, int off) {
        return ((b[off + 3] & 0xFF))
                + ((b[off + 2] & 0xFF) << 8)
                + ((b[off + 1] & 0xFF) << 16)
                + ((b[off]) << 24);
    }

    static float getFloat(byte[] b, int off) {
        return Float.intBitsToFloat(getInt(b, off));
    }

    static long getLong(byte[] b, int off) {
        return ((b[off + 7] & 0xFFL))
                + ((b[off + 6] & 0xFFL) << 8)
                + ((b[off + 5] & 0xFFL) << 16)
                + ((b[off + 4] & 0xFFL) << 24)
                + ((b[off + 3] & 0xFFL) << 32)
                + ((b[off + 2] & 0xFFL) << 40)
                + ((b[off + 1] & 0xFFL) << 48)
                + (((long) b[off]) << 56);
    }

    static double getDouble(byte[] b, int off) {
        return Double.longBitsToDouble(getLong(b, off));
    }

    static void putBoolean(byte[] b, int off, boolean val) {
        b[off] = (byte) (val ? 1 : 0);
    }

    static void putChar(byte[] b, int off, char val) {
        b[off + 1] = (byte) (val);
        b[off] = (byte) (val >>> 8);
    }

    static void putShort(byte[] b, int off, short val) {
        b[off + 1] = (byte) (val);
        b[off] = (byte) (val >>> 8);
    }

    static void putInt(byte[] b, int off, int val) {
        b[off + 3] = (byte) (val);
        b[off + 2] = (byte) (val >>> 8);
        b[off + 1] = (byte) (val >>> 16);
        b[off] = (byte) (val >>> 24);
    }

    static void putFloat(byte[] b, int off, float val) {
        putInt(b, off, Float.floatToIntBits(val));
    }

    static void putLong(byte[] b, int off, long val) {
        b[off + 7] = (byte) (val);
        b[off + 6] = (byte) (val >>> 8);
        b[off + 5] = (byte) (val >>> 16);
        b[off + 4] = (byte) (val >>> 24);
        b[off + 3] = (byte) (val >>> 32);
        b[off + 2] = (byte) (val >>> 40);
        b[off + 1] = (byte) (val >>> 48);
        b[off] = (byte) (val >>> 56);
    }

    static void putDouble(byte[] b, int off, double val) {
        putLong(b, off, Double.doubleToLongBits(val));
    }
}
