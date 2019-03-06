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

package org.apache.ignite.internal.processors.hadoop.io;

import org.apache.ignite.hadoop.io.RawMemory;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Offheap-based memory.
 */
public class OffheapRawMemory implements RawMemory {
    /** Pointer. */
    private long ptr;

    /** Length. */
    private int len;

    /**
     * Constructor.
     *
     * @param ptr Pointer.
     * @param len Length.
     */
    public OffheapRawMemory(long ptr, int len) {
        update(ptr, len);
    }

    /** {@inheritDoc} */
    @Override public byte get(int idx) {
        ensure(idx, 1);

        return GridUnsafe.getByte(ptr + idx);
    }

    /** {@inheritDoc} */
    @Override public short getShort(int idx) {
        ensure(idx, 2);

        return GridUnsafe.getShort(ptr + idx);
    }

    /** {@inheritDoc} */
    @Override public char getChar(int idx) {
        ensure(idx, 2);

        return GridUnsafe.getChar(ptr + idx);
    }

    /** {@inheritDoc} */
    @Override public int getInt(int idx) {
        ensure(idx, 4);

        return GridUnsafe.getInt(ptr + idx);
    }

    /** {@inheritDoc} */
    @Override public long getLong(int idx) {
        ensure(idx, 8);

        return GridUnsafe.getLong(ptr + idx);
    }

    /** {@inheritDoc} */
    @Override public float getFloat(int idx) {
        ensure(idx, 4);

        return GridUnsafe.getFloat(ptr + idx);
    }

    /** {@inheritDoc} */
    @Override public double getDouble(int idx) {
        ensure(idx, 8);

        return GridUnsafe.getDouble(ptr + idx);
    }

    /** {@inheritDoc} */
    @Override public int length() {
        return len;
    }

    /**
     * @return Raw pointer.
     */
    public long pointer() {
        return ptr;
    }

    /**
     * Update pointer and length.
     *
     * @param ptr Pointer.
     * @param len Length.
     */
    public void update(long ptr, int len) {
        this.ptr = ptr;
        this.len = len;
    }

    /**
     * Ensure that the given number of bytes are available for read. Throw an exception otherwise.
     *
     * @param idx Index.
     * @param cnt Count.
     */
    private void ensure(int idx, int cnt) {
        if (idx < 0 || idx + cnt - 1 >= len)
            throw new IndexOutOfBoundsException("Illegal index [len=" + len + ", idx=" + idx + ']');
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(OffheapRawMemory.class, this);
    }
}
