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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.math.primitives.vector.VectorStorage;

/**
 * Array based {@link VectorStorage} implementation.
 */
public class DenseVectorStorage implements VectorStorage {
    /** Raw data array. */
    private Serializable[] rawData;

    /** Numeric vector array */
    private double[] data;

    /**
     * IMPL NOTE required by {@link Externalizable}.
     */
    public DenseVectorStorage() {
        // No-op.
    }

    /**
     * @param size Vector size.
     */
    public DenseVectorStorage(int size) {
        assert size >= 0;

        data = new double[size];
    }

    /**
     * @param data Backing data array.
     */
    public DenseVectorStorage(double[] data) {
        assert data != null;

        this.data = data;
    }

    /**
     * @param data Backing data array.
     */
    public DenseVectorStorage(Serializable[] data) {
        assert data != null;

        this.rawData = data;
    }

    /** {@inheritDoc} */
    @Override public int size() {
        if (data == null && rawData == null)
            return 0;
        else
            return data != null ? data.length : rawData.length;
    }

    /**
     * Tries to cast internal representation of data to array of doubles if need.
     */
    private void toNumericArray() {
        A.ensure(data == null || rawData == null, "data == null || rawData == null");
        if (data == null && rawData == null)
            return;

        if (data == null) {
            data = new double[rawData.length];
            for (int i = 0; i < rawData.length; i++)
                data[i] = rawData[i] == null ? 0.0 : ((Number)rawData[i]).doubleValue(); //TODO: IGNITE-11664
            rawData = null;
        }
    }

    /**
     * Tries to cast internal representation of data to array of Serializable objects if need.
     */
    private void toGenericArray() {
        A.ensure(data == null || rawData == null, "data == null || rawData == null");
        if (data == null && rawData == null)
            return;

        if (rawData == null) {
            rawData = new Serializable[data.length];
            for (int i = 0; i < rawData.length; i++)
                rawData[i] = data[i];
            data = null;
        }
    }

    /** {@inheritDoc} */
    @Override public double get(int i) {
        if (data != null)
            return data[i];

        Serializable v = rawData[i];
        //TODO: IGNITE-11664
        return v == null ? 0.0 : ((Number)rawData[i]).doubleValue();
    }

    /** {@inheritDoc} */
    @Override public <T extends Serializable> T getRaw(int i) {
        toGenericArray();
        return (T)rawData[i];
    }

    /** {@inheritDoc} */
    @Override public void set(int i, double v) {
        if (data != null)
            data[i] = v;
        else
            rawData[i] = v;
    }

    /** {@inheritDoc} */
    @Override public void setRaw(int i, Serializable v) {
        toGenericArray();
        this.rawData[i] = v;
    }

    /** {@inheritDoc}} */
    @Override public boolean isArrayBased() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public double[] data() {
        if (!isNumeric())
            throw new ClassCastException("Vector has not only numeric values.");

        toNumericArray();
        return data;
    }

    /** {@inheritDoc} */
    @Override public Serializable[] rawData() {
        toGenericArray();
        return rawData;
    }

    /** {@inheritDoc} */
    @Override public boolean isDense() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean isNumeric() {
        if (data != null || rawData == null)
            return true;

        for (int i = 0; i < rawData.length; i++) {
            if (rawData[i] != null && !(rawData[i] instanceof Number))
                return false;
        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        boolean isRawVector = data == null;
        out.writeBoolean(isRawVector);

        if (data != null)
            out.writeObject(data);
        else
            out.writeObject(rawData);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        boolean isRawVector = in.readBoolean();
        if (isRawVector) {
            rawData = (Serializable[])in.readObject();
            data = null;
        }
        else {
            rawData = null;
            data = (double[])in.readObject();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DenseVectorStorage storage = (DenseVectorStorage)o;
        return Arrays.equals(rawData, storage.rawData) &&
            Arrays.equals(data, storage.data);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = Arrays.hashCode(rawData);
        res = 31 * res + Arrays.hashCode(data);
        return res;
    }

    /**
     * Returns array of raw values.
     *
     * @return Raw data array.
     */
    Serializable[] getRawData() {
        return rawData;
    }

    /**
     * Returns array of double values.
     *
     * @return Numeric data array.
     */
    double[] getData() {
        return data;
    }
}
