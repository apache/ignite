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
import org.apache.ignite.ml.math.VectorStorage;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.functions.IntDoubleToVoidFunction;

/**
 * Read-only or read-write function-based vector storage.
 */
public class FunctionVectorStorage implements VectorStorage {
    /** */
    private IgniteFunction<Integer, Double> getFunc;
    /** */
    private IntDoubleToVoidFunction setFunc;
    /** */
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
    public FunctionVectorStorage(int size, IgniteFunction<Integer, Double> getFunc, IntDoubleToVoidFunction setFunc) {
        assert size > 0;
        assert getFunc != null; // At least get function is required.

        this.size = size;
        this.getFunc = getFunc;
        this.setFunc = setFunc;
    }

    /**
     * @return Getter function.
     */
    public IgniteFunction<Integer, Double> getFunction() {
        return getFunc;
    }

    /**
     * @return Setter function.
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
    public FunctionVectorStorage(int size, IgniteFunction<Integer, Double> getFunc) {
        this(size, getFunc, null);
    }

    /** {@inheritDoc} */
    @Override public int size() {
        return size;
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        return getFunc.apply(i);
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        if (setFunc != null)
            setFunc.accept(i, v);
        else
            throw new UnsupportedOperationException("Cannot set into read-only vector.");
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(setFunc);
        out.writeObject(getFunc);
        out.writeInt(size);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        setFunc = (IntDoubleToVoidFunction)in.readObject();
        getFunc = (IgniteFunction<Integer, Double>)in.readObject();
        size = in.readInt();
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
    @Override public boolean isRandomAccess() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDistributed() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isArrayBased() {
        return false;
    }
}
