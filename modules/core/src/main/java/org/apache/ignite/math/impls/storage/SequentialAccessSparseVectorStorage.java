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
 * TODO wip
 */
public class SequentialAccessSparseVectorStorage implements VectorStorage {
    private static final double DEFAULT_VALUE = 0.0;
    private Int2DoubleRBTreeMap data;

    /**
     * If true, doesn't allow DEFAULT_VALUE in the mapping (adding a zero discards it).
     * Otherwise, a DEFAULT_VALUE is treated like any other value.
     */
    private boolean noDefault = true;

    /** For serialization. */
    public SequentialAccessSparseVectorStorage(){
        // No-op.
    }

    /**
     *
     * @param keys
     * @param values
     * @param noDefault
     */
    public SequentialAccessSparseVectorStorage(int[] keys, double[] values, boolean noDefault) {
        this.noDefault = noDefault;
        this.data = new Int2DoubleRBTreeMap(keys, values);
    }

    /**
     *
     * @param storage
     * @param noDefault
     */
    private SequentialAccessSparseVectorStorage(VectorStorage storage, boolean noDefault) {
        this.data = new Int2DoubleRBTreeMap();
        this.noDefault = noDefault;

        for (int i = 0; i < storage.size(); i++)
            data.put(i, storage.get(i));
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
        data.put(i, v); // TODO wip, default/nodefault
    }

    /** {@inheritDoc} */
    @Override public double[] data() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // TODO wip
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // TODO wip
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

    private boolean nonDefault(double x) {
        return Double.compare(x, DEFAULT_VALUE) != 0 || !noDefault;
    }

    /** {@inheritDoc} */
    @Override public boolean isAddConstantTime() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }

    @Override public boolean equals(Object obj) {
        return super.equals(obj); //TODO
    }

    @Override public int hashCode() {
        return super.hashCode(); //TODO
    }
}
