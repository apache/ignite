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
import java.util.Random;
import org.apache.ignite.ml.math.MurmurHash;
import org.apache.ignite.ml.math.VectorStorage;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;

/**
 * {@link VectorStorage} implementation with random values in the vector elements.
 */
public class RandomVectorStorage implements VectorStorage {
    /** */
    private static final long SCALE = 1L << 32;
    /** */
    private static final int PRIME = 104047;

    /** Random generation seed. */
    private int seed;

    /** Vector size. */
    private int size;

    /** Whether fast hash is used, in {@link #get(int)}. */
    private boolean fastHash;

    /** */
    public RandomVectorStorage() {
        // No-op.
    }

    /**
     * @param size Size of the storage.
     * @param fastHash Whether or not to use fast hashing or Murmur hashing.
     */
    public RandomVectorStorage(int size, boolean fastHash) {
        assert size > 0;

        this.size = size;
        this.fastHash = fastHash;

        seed = new Random().nextInt();
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        if (!fastHash) {
            ByteBuffer buf = ByteBuffer.allocate(4);

            buf.putInt(i);
            buf.flip();

            return (MurmurHash.hash64A(buf, seed) & (SCALE - 1)) / (double)SCALE;
        }
        else
            // This isn't a fantastic random number generator, but it is just fine for random projections.
            return (((i * PRIME) & 8) * 0.25) - 1;
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        throw new UnsupportedOperationException("Random vector storage is a read-only storage.");
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(size);
        out.writeInt(seed);
        out.writeBoolean(fastHash);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        size = in.readInt();
        seed = in.readInt();
        fastHash = in.readBoolean();
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
    @Override public boolean isArrayBased() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + Boolean.hashCode(fastHash);
        res = res * 37 + seed;
        res = res * 37 + size;

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        RandomVectorStorage that = (RandomVectorStorage)o;

        return size == that.size && seed == that.seed && fastHash == that.fastHash;
    }
}
