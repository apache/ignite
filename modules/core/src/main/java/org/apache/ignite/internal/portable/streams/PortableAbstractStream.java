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

package org.apache.ignite.internal.portable.streams;

import org.apache.ignite.internal.util.*;

import sun.misc.*;

import java.nio.*;

/**
 * Portable abstract stream.
 */
public abstract class PortableAbstractStream implements PortableStream {
    /** Byte: zero. */
    protected static final byte BYTE_ZERO = 0;

    /** Byte: one. */
    protected static final byte BYTE_ONE = 1;

    /** Whether little endian is used on the platform. */
    protected static final boolean LITTLE_ENDIAN = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;

    /** Unsafe instance. */
    protected static final Unsafe UNSAFE = GridUnsafe.unsafe();

    /** Array offset: boolean. */
    protected static final long BOOLEAN_ARR_OFF = UNSAFE.arrayBaseOffset(boolean[].class);

    /** Array offset: byte. */
    protected static final long BYTE_ARR_OFF = UNSAFE.arrayBaseOffset(byte[].class);

    /** Array offset: short. */
    protected static final long SHORT_ARR_OFF = UNSAFE.arrayBaseOffset(short[].class);

    /** Array offset: char. */
    protected static final long CHAR_ARR_OFF = UNSAFE.arrayBaseOffset(char[].class);

    /** Array offset: int. */
    protected static final long INT_ARR_OFF = UNSAFE.arrayBaseOffset(int[].class);

    /** Array offset: float. */
    protected static final long FLOAT_ARR_OFF = UNSAFE.arrayBaseOffset(float[].class);

    /** Array offset: long. */
    protected static final long LONG_ARR_OFF = UNSAFE.arrayBaseOffset(long[].class);

    /** Array offset: double. */
    protected static final long DOUBLE_ARR_OFF = UNSAFE.arrayBaseOffset(double[].class);

    /** Position. */
    protected int pos;

    /** {@inheritDoc} */
    @Override public int position() {
        return pos;
    }

    /**
     * Shift position.
     *
     * @param cnt Byte count.
     */
    protected void shift(int cnt) {
        pos += cnt;
    }
}
