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

package org.apache.ignite.internal.processors.hadoop.shuffle.mem;

/**
 * Memory manager interface
 */
public abstract class MemoryManager {
    /**
     * @param valPtr Value page pointer.
     * @param nextValPtr Next value page pointer.
     */
    public void nextValue(long valPtr, long nextValPtr) {
        writeLong(valPtr, nextValPtr);
    }

    /**
     * @param valPtr Value page pointer.
     * @return Next value page pointer.
     */
    public long nextValue(long valPtr) {
        return readLong(valPtr);
    }

    /**
     * @param valPtr Value page pointer.
     * @param size Size.
     */
    public void valueSize(long valPtr, int size) {
        writeInt(valPtr + 8, size);
    }

    /**
     * @param valPtr Value page pointer.
     * @return Value size.
     */
    public int valueSize(long valPtr) {
        return readInt(valPtr + 8);
    }

    /**
     * @return Page size.
     */
    public abstract int pageSize();

    /**
     * Close manager.
     */
    public abstract void close();

    /**
     * Allocates memory of given size in bytes.
     *
     * @param size Size of allocated block.
     * @return Allocated block address.
     */
    public abstract long allocate(long size);

    /**
     * Copy memory.
     *
     * @param srcPtr Source pointer.
     * @param destPtr Destination pointer.
     * @param len Length in bytes.
     */
    public abstract void copyMemory(long srcPtr, long destPtr, long len);

    /**
     * Copy memory.
     *
     * @param srcBuf Source buffer.
     * @param srcOff Offset at the sources buffer.
     * @param destPtr Destination pointer.
     * @param len Length in bytes.
     */
    public abstract void copyMemory(byte[] srcBuf, int srcOff, long destPtr, long len);

    /**
     * Copy memory.
     *
     * @param srcPtr Source pointer.
     * @param dstBuf Destination byte array buffer.
     * @param dstOff Destination offset.
     * @param len Length in bytes.
     */
    public abstract void copyMemory(long srcPtr, byte[] dstBuf, int dstOff, long len);

    /**
     * Get bytes array.
     *
     * @param ptr Pointer.
     * @param len Length in bytes.
     * @return Bytes object. {@code null} if not supported.
     */
    public abstract Bytes bytes(long ptr, long len);

    /**
     * @param ptr Pointer.
     * @return Long value.
     */
    public abstract long readLongVolatile(long ptr);

    /**
     * @param ptr Pointer.
     * @param v Long value.
     */
    public abstract void writeLongVolatile(long ptr, long v);

    /**
     * @param ptr Pointer.
     * @param exp Expected.
     * @param v New value.
     * @return {@code true} If operation succeeded.
     */
    public abstract boolean casLong(long ptr, long exp, long v);

    /**
     * @param ptr Pointer.
     * @return Long value.
     */
    public abstract long readLong(long ptr);

    /**
     * @param ptr Pointer.
     * @param v Long value.
     */
    public abstract void writeLong(long ptr, long v);

    /**
     * @param ptr Pointer.
     * @return Integer value.
     */
    public abstract int readInt(long ptr);

    /**
     * @param ptr Pointer.
     * @param v Integer value.
     */
    public abstract void writeInt(long ptr, int v);

    /**
     * @param ptr Pointer.
     * @return Float value.
     */
    public abstract float readFloat(long ptr);

    /**
     * @param ptr Pointer.
     * @param v Value.
     */
    public abstract void writeFloat(long ptr, float v);

    /**
     * @param ptr Pointer.
     * @return Double value.
     */
    public abstract double readDouble(long ptr);

    /**
     * @param ptr Pointer.
     * @param v Value.
     */
    public abstract void writeDouble(long ptr, double v);

    /**
     * @param ptr Pointer.
     * @return Short value.
     */
    public abstract short readShort(long ptr);

    /**
     * @param ptr Pointer.
     * @param v Short value.
     */
    public abstract void writeShort(long ptr, short v);

    /**
     * @param ptr Pointer.
     * @return Integer value.
     */
    public abstract byte readByte(long ptr);

    /**
     * @param ptr Pointer.
     * @param v Integer value.
     */
    public abstract void writeByte(long ptr, byte v);

    /**
     * Writes part of byte array into memory location.
     *
     * @param ptr Pointer.
     * @param arr Array.
     * @param off Offset.
     * @param len Length.
     */
    public abstract void writeBytes(long ptr, byte[] arr, int off, int len);

    /**
     * @param ptr Pointer.
     * @param arr Array.
     * @param off Offset.
     * @param len Length.
     * @return The same array as passed in one.
     */
    public abstract byte[] readBytes(long ptr, byte[] arr, int off, int len);

    /**
     *
     */
    public static class Bytes {
        /** Buffer. */
        private final byte [] buf;
        /** Offset. */
        private final int off;
        /** Length. */
        private final int len;

        /**
         * @param buf Buffer.
         * @param off Offset.
         * @param len Length in bytes.
         */
        public Bytes(byte[] buf, int off, int len) {
            this.buf = buf;
            this.off = off;
            this.len = len;
        }

        /**
         * @return Buffer.
         */
        public byte[] buf() {
            return buf;
        }

        /**
         * @return Offset.
         */
        public int off() {
            return off;
        }

        /**
         * @return Length.
         */
        public int len() {
            return len;
        }
    }
}