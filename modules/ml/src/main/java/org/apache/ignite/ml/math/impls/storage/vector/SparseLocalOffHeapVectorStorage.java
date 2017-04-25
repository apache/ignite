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

package org.apache.ignite.ml.math.impls.storage.vector;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import org.apache.ignite.internal.util.offheap.GridOffHeapMap;
import org.apache.ignite.internal.util.offheap.GridOffHeapMapFactory;
import org.apache.ignite.ml.math.VectorStorage;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.impls.vector.SparseLocalOffHeapVector;

/**
 * {@link VectorStorage} implementation for {@link SparseLocalOffHeapVector}.
 */
public class SparseLocalOffHeapVectorStorage implements VectorStorage {
    /** Assume 10% density. */
    private static final int INIT_DENSITY = 10;

    /** Storage capacity. */
    private int size;

    /** Local off heap map. */
    private GridOffHeapMap gridOffHeapMap;

    /** */
    public SparseLocalOffHeapVectorStorage() {
        //No-op.
    }

    /**
     * @param cap Initial capacity.
     */
    public SparseLocalOffHeapVectorStorage(int cap) {
        assert cap > 0;

        gridOffHeapMap = GridOffHeapMapFactory.unsafeMap(cap / INIT_DENSITY);
        size = cap;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        byte[] bytes = gridOffHeapMap.get(hash(i), intToByteArray(i));
        return bytes == null ? 0 : ByteBuffer.wrap(bytes).getDouble();
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        if (v != 0.0)
            gridOffHeapMap.put(hash(i), intToByteArray(i), doubleToByteArray(v));
        else if (gridOffHeapMap.contains(hash(i), intToByteArray(i)))
            gridOffHeapMap.remove(hash(i), intToByteArray(i));
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        throw new UnsupportedOperationException(); // TODO: add externalization support.
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isRandomAccess() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        gridOffHeapMap.destruct();
    }

    /** */
    private int hash(int h) {
        // Apply base step of MurmurHash; see http://code.google.com/p/smhasher/
        // Despite two multiplies, this is often faster than others
        // with comparable bit-spread properties.
        h ^= h >>> 16;
        h *= 0x85ebca6b;
        h ^= h >>> 13;
        h *= 0xc2b2ae35;

        return (h >>> 16) ^ h;
    }

    /** */
    private byte[] intToByteArray(int val) {
        return new byte[] {
            (byte)(val >>> 24),
            (byte)(val >>> 16),
            (byte)(val >>> 8),
            (byte)val};
    }

    /** */
    private byte[] doubleToByteArray(double val) {
        long l = Double.doubleToRawLongBits(val);
        return new byte[] {
            (byte)((l >> 56) & 0xff),
            (byte)((l >> 48) & 0xff),
            (byte)((l >> 40) & 0xff),
            (byte)((l >> 32) & 0xff),
            (byte)((l >> 24) & 0xff),
            (byte)((l >> 16) & 0xff),
            (byte)((l >> 8) & 0xff),
            (byte)((l) & 0xff),
        };
    }
}
