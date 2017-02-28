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

/**
 * Vector storage holding a single non-zero value at some index.
 */
public class SingleElementVectorStorage implements VectorStorage {
    private int idx;
    private double val;
    private int size;

    /**
     *
     */
    public SingleElementVectorStorage() {
        // No-op.
    }

    /**
     *
     * @param size
     * @param idx
     * @param val
     */
    public SingleElementVectorStorage(int size, int idx, double val) {
        this.size = size;
        this.idx = idx;
        this.val = val;
    }

    public int index() {
        return idx;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public double get(int i) {
        return i == idx ? val : 0.0;
    }

    @Override
    public void set(int i, double v) {
        if (i == idx)
            val = v;
        else
            throw new UnsupportedOperationException("Can't set element outside of index: " + idx);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(size);
        out.writeInt(idx);
        out.writeDouble(val);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        size = in.readInt();
        idx = in.readInt();
        val = in.readDouble();
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
}
