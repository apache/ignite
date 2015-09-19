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

package org.apache.ignite.internal.processors.hadoop.shuffle.streams;

/**
 * Offheap buffer.
 */
public class HadoopOffheapBuffer {
    /** Buffer begin address. */
    private long bufPtr;

    /** The first address we do not own. */
    private long bufEnd;

    /** Current read or write pointer. */
    private long posPtr;

    /**
     * @param bufPtr Pointer to buffer begin.
     * @param bufSize Size of the buffer.
     */
    public HadoopOffheapBuffer(long bufPtr, long bufSize) {
        set(bufPtr, bufSize);
    }

    /**
     * @param bufPtr Pointer to buffer begin.
     * @param bufSize Size of the buffer.
     */
    public void set(long bufPtr, long bufSize) {
        this.bufPtr = bufPtr;

        posPtr = bufPtr;
        bufEnd = bufPtr + bufSize;
    }

    /**
     * @return Pointer to internal buffer begin.
     */
    public long begin() {
        return bufPtr;
    }

    /**
     * @return Buffer capacity.
     */
    public long capacity() {
        return bufEnd - bufPtr;
    }

    /**
     * @return Remaining capacity.
     */
    public long remaining() {
        return bufEnd - posPtr;
    }

    /**
     * @return Absolute pointer to the current position inside of the buffer.
     */
    public long pointer() {
        return posPtr;
    }

    /**
     * @param ptr Absolute pointer to the current position inside of the buffer.
     */
    public void pointer(long ptr) {
        assert ptr >= bufPtr : bufPtr + " <= " + ptr;
        assert ptr <= bufEnd : bufEnd + " <= " + bufPtr;

        posPtr = ptr;
    }

    /**
     * @param size Size move on.
     * @return Old position pointer or {@code 0} if move goes beyond the end of the buffer.
     */
    public long move(long size) {
        assert size > 0 : size;

        long oldPos = posPtr;
        long newPos = oldPos + size;

        if (newPos > bufEnd)
            return 0;

        posPtr = newPos;

        return oldPos;
    }

    /**
     * @param ptr Pointer.
     * @return {@code true} If the given pointer is inside of this buffer.
     */
    public boolean isInside(long ptr) {
        return ptr >= bufPtr && ptr <= bufEnd;
    }

    /**
     * Resets position to the beginning of buffer.
     */
    public void reset() {
        posPtr = bufPtr;
    }
}