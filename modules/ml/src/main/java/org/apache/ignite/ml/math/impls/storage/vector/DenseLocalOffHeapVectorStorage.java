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
import java.util.stream.IntStream;
import org.apache.ignite.internal.util.GridUnsafe;
import org.apache.ignite.ml.math.VectorStorage;

/**
 * Local, dense off-heap vector storage.
 */
public class DenseLocalOffHeapVectorStorage implements VectorStorage {
    /** Vector size. */
    private int size;

    /** */
    private transient long ptr;
    //TODO: temp solution.
    /** */
    private int ptrInitHash;

    /**
     *
     */
    public DenseLocalOffHeapVectorStorage() {
        // No-op.
    }

    /**
     * @param size Vector size.
     */
    public DenseLocalOffHeapVectorStorage(int size) {
        assert size > 0;

        this.size = size;

        allocateMemory(size);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        return GridUnsafe.getDouble(pointerOffset(i));
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        GridUnsafe.putDouble(pointerOffset(i), v);
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public double[] data() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isRandomAccess() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(size);
        out.writeInt(ptrInitHash);

        for (int i = 0; i < size; i++)
            out.writeDouble(get(i));
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        size = in.readInt();

        allocateMemory(size);

        ptrInitHash = in.readInt();

        for (int i = 0; i < size; i++)
            set(i, in.readDouble());
    }

    /** {@inheritDoc} */
    @Override public void destroy() {
        GridUnsafe.freeMemory(ptr);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + size;
        res = res * 37 + ptrInitHash;

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        DenseLocalOffHeapVectorStorage that = (DenseLocalOffHeapVectorStorage)o;

        return size == that.size && isMemoryEquals(that);
    }

    /** */
    private boolean isMemoryEquals(DenseLocalOffHeapVectorStorage otherStorage) {
        return IntStream.range(0, size).parallel().noneMatch(idx -> Double.compare(get(idx), otherStorage.get(idx)) != 0);
    }

    /**
     * Pointer offset for specific index.
     *
     * @param i Offset index.
     */
    private long pointerOffset(int i) {
        return ptr + i * Double.BYTES;
    }

    /** */
    private void allocateMemory(int size) {
        ptr = GridUnsafe.allocateMemory(size * Double.BYTES);

        ptrInitHash = Long.hashCode(ptr);
    }
}
