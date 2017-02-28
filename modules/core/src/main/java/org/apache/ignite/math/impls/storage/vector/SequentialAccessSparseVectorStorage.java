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

import it.unimi.dsi.fastutil.ints.*;
import org.apache.ignite.math.*;
import java.io.*;
import java.util.stream.*;

/**
 * Sequential access optimized sparse on-heap vector storage.
 */
public class SequentialAccessSparseVectorStorage implements VectorStorage {
    /** */
    private static final double DEFAULT_VALUE = 0.0;
    /** */
    private Int2DoubleRBTreeMap data;

    /**
     * If true, doesn't allow DEFAULT_VALUE in the mapping (adding a zero discards it).
     * Otherwise, a DEFAULT_VALUE is treated like any other value.
     */
    private boolean noDefault = true;

    /** For serialization. */
    public SequentialAccessSparseVectorStorage(){
        this.data = new Int2DoubleRBTreeMap();
    }

    /**
     *
     * @param noDefault
     */
    public SequentialAccessSparseVectorStorage(boolean noDefault){
        this.noDefault = noDefault;

        data = new Int2DoubleRBTreeMap();
    }

    /**
     *
     * @param keys
     * @param values
     * @param noDefault
     */
    public SequentialAccessSparseVectorStorage(int[] keys, double[] values, boolean noDefault) {
        this.noDefault = noDefault;

        data = new Int2DoubleRBTreeMap(keys, values);
    }

    /**
     *
     * @param storage
     * @param noDefault
     */
    public SequentialAccessSparseVectorStorage(VectorStorage storage, boolean noDefault) {
        this.noDefault = noDefault;

        data = new Int2DoubleRBTreeMap();

        for (int i = 0; i < storage.size(); i++)
            data.put(i, storage.get(i));
    }

    /**
     *
     * @param data
     * @param noDefault
     */
    public SequentialAccessSparseVectorStorage(Int2DoubleRBTreeMap data, boolean noDefault){
        this.noDefault = noDefault;
        this.data = data;
    }

    /**
     *
     * @param arrs
     * @param noDefault
     */
    public SequentialAccessSparseVectorStorage(double[] arrs, boolean noDefault) {
        this.noDefault = noDefault;

        data = new Int2DoubleRBTreeMap();

        IntStream.range(0,arrs.length).forEachOrdered(i -> data.put(i, arrs[i]));
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return data.size();
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        return data.get(i);
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        if (noDefault && isDefaultValue(v) && isDefaultValue(get(i)))
            return;

        data.put(i, v);
    }

    /** {@inheritDoc} */
    @Override public double[] data() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(noDefault);
        out.writeObject(data);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        noDefault = in.readBoolean();
        data = (Int2DoubleRBTreeMap)in.readObject();
    }

    /** {@inheritDoc} */
    @Override public boolean isSequentialAccess() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override public double getLookupCost() {
        return Math.max(1, Math.round(Functions.LOG2.apply(data.values().stream().filter(this::nonDefault).count())));
    }

    /** */
    private boolean nonDefault(double x) {
        return !isDefaultValue(x) || !noDefault;
    }

    /** */
    private boolean isDefaultValue(double x) {
        return Double.compare(x, DEFAULT_VALUE) == 0;
    }

    /** {@inheritDoc} */
    @Override public boolean isAddConstantTime() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    /** {@inheritDoc} */
    @Override protected Object clone() throws CloneNotSupportedException {
        return new SequentialAccessSparseVectorStorage(data.clone(), noDefault);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        return obj != null &&
            getClass().equals(obj.getClass()) &&
            (noDefault == ((SequentialAccessSparseVectorStorage)obj).noDefault) &&
            data.equals(((SequentialAccessSparseVectorStorage)obj).data);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = 1;

        result = result * 37 + Boolean.hashCode(noDefault);
        result = result * 37 + data.hashCode();

        return result;
    }
}
