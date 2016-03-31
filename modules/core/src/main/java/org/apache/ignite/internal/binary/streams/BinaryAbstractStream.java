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

package org.apache.ignite.internal.binary.streams;

/**
 * Binary abstract stream.
 */
public abstract class BinaryAbstractStream implements BinaryStream {
    /** Byte: zero. */
    protected static final byte BYTE_ZERO = 0;

    /** Byte: one. */
    protected static final byte BYTE_ONE = 1;

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
