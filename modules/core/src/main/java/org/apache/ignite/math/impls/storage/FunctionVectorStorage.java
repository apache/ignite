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

import org.apache.ignite.math.*;
import org.apache.ignite.math.UnsupportedOperationException;
import java.io.*;
import java.util.function.*;

/**
 * Read-only or read-write function-based vector storage.
 */
public class FunctionVectorStorage implements VectorStorage {
    private IntToDoubleFunction getFunc;
    private IntDoubleToVoidFunction setFunc;
    private int size;

    /**
     *
     */
    public FunctionVectorStorage() {
        // No-op.
    }

    /**
     * Creates read-only or read-write storage.
     *
     * @param size Cardinality of this vector storage.
     * @param getFunc Get function.
     * @param setFunc Optional set function ({@code null} for read-only storage).
     */
    public FunctionVectorStorage(int size, IntToDoubleFunction getFunc, IntDoubleToVoidFunction setFunc) {
        assert getFunc != null; // At least get function is required.

        this.size = size;
        this.getFunc = getFunc;
        this.setFunc = setFunc;
    }

    /**
     *
     * @return
     */
    public IntToDoubleFunction getFunction() {
        return getFunc;
    }

    /**
     *
     * @return
     */
    public IntDoubleToVoidFunction setFunction() {
        return setFunc;
    }

    /**
     * Creates read-only storage.
     *
     * @param size Cardinality of this vector storage.
     * @param getFunc Get function.
     */
    public FunctionVectorStorage(int size, IntToDoubleFunction getFunc) {
        this(size, getFunc, null);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public double get(int i) {
        return getFunc.applyAsDouble(i);
    }

    @Override
    public void set(int i, double v) {
        if (setFunc != null)
            setFunc.apply(i, v);
        else
            throw new UnsupportedOperationException("Cannot set into read-only vector.");
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(setFunc);
        out.writeObject(getFunc);
        out.writeInt(size);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setFunc = (IntDoubleToVoidFunction)in.readObject();
        getFunc = (IntToDoubleFunction)in.readObject();
        size = in.readInt();
    }

    @Override
    public boolean isSequentialAccess() {
        return false;
    }

    @Override
    public boolean isDense() {
        return false;
    }

    @Override
    public double getLookupCost() {
        return 0;
    }

    @Override
    public boolean isAddConstantTime() {
        return false;
    }

    @Override
    public boolean isArrayBased() {
        return false;
    }
}
