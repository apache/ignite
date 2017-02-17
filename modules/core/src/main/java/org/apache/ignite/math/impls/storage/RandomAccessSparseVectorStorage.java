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

package org.apache.ignite.math.impls.storage;

import it.unimi.dsi.fastutil.ints.*;
import org.apache.ignite.math.*;
import java.io.*;

/**
 * Implements vector that only stores non-zero doubles.
 */
public class RandomAccessSparseVectorStorage implements VectorStorage{
    private int size;
    private Int2DoubleOpenHashMap data;

    private static final int INITIAL_CAPACITY = 11;

    /** For serialization. */
    public RandomAccessSparseVectorStorage(){
        // No-op.
    }

    /**
     *
     * @param size
     */
    public RandomAccessSparseVectorStorage(int size){
        this(size, Math.min(size, INITIAL_CAPACITY));
    }

    /**
     *
     * @param size
     * @param initCap
     */
    public RandomAccessSparseVectorStorage(int size, int initCap) {
        this.size = size;
        this.data = new Int2DoubleOpenHashMap(initCap, .5f);
    }

    private RandomAccessSparseVectorStorage(int size, Int2DoubleOpenHashMap values) {
        this.size = size;
        this.data = values;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        return data.get(i);
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        if (v == 0.0)
            data.remove(i);
        else
            data.put(i, v);
    }

    /** {@inheritDoc} */
    @Override public double[] data() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(size);
        out.writeInt(data.size());

        for (Int2DoubleMap.Entry entry : data.int2DoubleEntrySet()) {
            out.writeInt(entry.getIntKey());
            out.writeDouble(entry.getDoubleValue());
        }
    }



    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        size = in.readInt();

        data = new Int2DoubleOpenHashMap(size, .5f);

        int actualSize = in.readInt();
        for (int i = 0; i < actualSize; i++) {
            data.put(in.readInt(), in.readDouble());
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public double getLookupCost() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override public boolean isAddConstantTime() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    @Override protected Object clone() throws CloneNotSupportedException {
        return new RandomAccessSparseVectorStorage(size, data.clone());
    }

    @Override public boolean equals(Object obj) {
        return obj != null && getClass().equals(obj.getClass()) &&
            (size == ((RandomAccessSparseVectorStorage)obj).size) && data.equals(((RandomAccessSparseVectorStorage)obj).data);
    }

    @Override public int hashCode() {
        int result = 1;
        result = 37 * result + size;
        result = 37 * result + data.hashCode();
        return result;
    }
}
