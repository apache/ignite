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

package org.apache.ignite.math.impls;

import java.io.*;
import java.util.*;

/**
 * TODO: add description.
 */
class VectorArrayStorage implements VectorStorage {
    /** */
    private double[] data;

    /** */
    @Override public boolean equals(Object o) {
        return this == o || !(o == null || getClass() != o.getClass())
            && Arrays.equals(data, ((VectorArrayStorage)o).data);
    }

    /**
     * IMPL NOTE required by Externalizable
     */
    public VectorArrayStorage() {
        this(null);
    }

    /**
     *
     * @param size
     */
    VectorArrayStorage(int size) {
        data = new double[size];
    }

    /**
     *
     * @param data
     */
    VectorArrayStorage(double[] data) {
        this.data = data;
    }

    /** {@inheritDoc */
    @Override public int size() {
        return data == null ? 0 : data.length;
    }

    /** {@inheritDoc */
    @Override public double get(int i) {
        return data[i];
    }

    /** {@inheritDoc */
    @Override public void set(int i, double v) {
        data[i] = v;
    }

    /** {@inheritDoc */
    @Override public boolean isArrayBased() {
        return true;
    }

    /** {@inheritDoc */
    @Override public double[] data() {
        return data;
    }

    /** {@inheritDoc */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(data);
    }

    /** {@inheritDoc */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        data = (double[])in.readObject();
    }
}
