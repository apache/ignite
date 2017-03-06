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

package org.apache.ignite.math.impls.storage.vector;

import org.apache.ignite.math.*;
import org.apache.ignite.math.UnsupportedOperationException;
import java.io.*;
import java.nio.*;
import java.util.*;

/**
 *
 */
public class RandomVectorStorage implements VectorStorage {
    private static final long SCALE = 1L << 32;
    private static final int PRIME = 104047;

    private int seed;
    private int size;
    private boolean fastHash;

    /** */
    public RandomVectorStorage(){
        // No-op.
    }

    /**
     *
     * @param size Size of the storage.
     * @param fastHash Whether or not to use fast hashing or Murmur hashing.
     */
    public RandomVectorStorage(int size, boolean fastHash) {
        assert size > 0;

        this.size = size;
        this.fastHash = fastHash;

        seed = new Random().nextInt();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public double get(int i) {
        if (!fastHash) {
            ByteBuffer buf = ByteBuffer.allocate(4);

            buf.putInt(i);
            buf.flip();

            return (MurmurHash.hash64A(buf, seed) & (SCALE - 1)) / (double)SCALE;
        } else
            // This isn't a fantastic random number generator, but it is just fine for random projections.
            return (((i * PRIME) & 8) * 0.25) - 1;
    }

    @Override
    public void set(int i, double v) {
        throw new UnsupportedOperationException("Random vector storage is a read-only storage.");
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(size);
        out.writeInt(seed);
        out.writeBoolean(fastHash);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        size = in.readInt();
        seed = in.readInt();
        fastHash = in.readBoolean();
    }

    @Override
    public boolean isSequentialAccess() {
        return true;
    }

    @Override
    public boolean isDense() {
        return true;
    }

    @Override
    public double getLookupCost() {
        return 0;
    }

    @Override
    public boolean isAddConstantTime() {
        return true;
    }

    @Override
    public boolean isArrayBased() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = 1;

        result = result * 37 + Boolean.hashCode(fastHash);
        result = result * 37 + seed;
        result = result * 37 + size;

        return result;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        RandomVectorStorage that = (RandomVectorStorage) o;

        return size == that.size && seed == that.seed && fastHash == that.fastHash;
    }
}
