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

package org.apache.ignite.ml.math.primitives.vector.storage;

import it.unimi.dsi.fastutil.ints.Int2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2DoubleRBTreeMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.ml.math.StorageConstants;
import org.apache.ignite.ml.math.primitives.vector.VectorStorage;

/**
 * Sparse, local, on-heap vector storage.
 */
public class SparseVectorStorage implements VectorStorage, StorageConstants {
    /** */
    private int size;
    /** */
    private int acsMode;

    /** Actual map storage. */
    private Map<Integer, Double> sto;

    /**
     *
     */
    public SparseVectorStorage() {
        // No-op.
    }

    /** */
    public SparseVectorStorage(Map<Integer, Double> map, boolean cp) {
        assert map.size() > 0;

        this.size = map.size();

        if (map instanceof Int2DoubleRBTreeMap)
            acsMode = SEQUENTIAL_ACCESS_MODE;
        else if (map instanceof Int2DoubleOpenHashMap)
            acsMode = RANDOM_ACCESS_MODE;
        else
            acsMode = UNKNOWN_STORAGE_MODE;

        if (cp)
            switch (acsMode) {
                case SEQUENTIAL_ACCESS_MODE:
                    sto = new Int2DoubleRBTreeMap(map);
                case RANDOM_ACCESS_MODE:
                    sto = new Int2DoubleOpenHashMap(map);
                    break;
                default:
                    sto = new HashMap<>(map);
            }
        else
            sto = map;
    }

    /**
     * @param size Vector size.
     * @param acsMode Access mode.
     */
    public SparseVectorStorage(int size, int acsMode) {
        assert size > 0;
        assertAccessMode(acsMode);

        this.size = size;
        this.acsMode = acsMode;

        if (acsMode == SEQUENTIAL_ACCESS_MODE)
            sto = new Int2DoubleRBTreeMap();
        else
            sto = new Int2DoubleOpenHashMap();
    }

    /**
     * @return Vector elements access mode.
     */
    public int getAccessMode() {
        return acsMode;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        return sto.getOrDefault(i, 0.0);
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        if (v != 0.0)
            sto.put(i, v);
        else if (sto.containsKey(i))
            sto.remove(i);

    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(size);
        out.writeInt(acsMode);
        out.writeObject(sto);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        size = in.readInt();
        acsMode = in.readInt();
        sto = (Map<Integer, Double>)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return acsMode == SEQUENTIAL_ACCESS_MODE;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return false;
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
    @Override public double[] data() {
        double[] data = new double[size];

        sto.forEach((idx, val) -> data[idx] = val);

        return data;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;

        if (o == null || getClass() != o.getClass())
            return false;

        SparseVectorStorage that = (SparseVectorStorage)o;

        return size == that.size && acsMode == that.acsMode && (sto != null ? sto.equals(that.sto) : that.sto == null);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = size;

        res = 31 * res + acsMode;
        res = 31 * res + (sto != null ? sto.hashCode() : 0);

        return res;
    }

    /** */
    public IntSet indexes() {
        return (IntSet)sto.keySet();
    }
}
