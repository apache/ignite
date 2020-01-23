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

package org.apache.ignite.ml.structures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import org.apache.ignite.ml.math.exceptions.math.IndexException;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/** Class to keep one observation in dataset. This is a base class for labeled and unlabeled rows. */
public class DatasetRow<V extends Vector> implements Externalizable {
    /** Vector. */
    protected V vector;

    /**
     * Default constructor (required by Externalizable).
     */
    public DatasetRow() {
    }

    /** */
    public DatasetRow(V vector) {
        this.vector = vector;
    }

    /**
     * Get the vector.
     *
     * @return Vector.
     */
    public V features() {
        return vector;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        DatasetRow vector1 = (DatasetRow)o;

        return vector != null ? vector.equals(vector1.vector) : vector1.vector == null;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return vector != null ? vector.hashCode() : 0;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(vector);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        vector = (V)in.readObject();
    }

    /**
     * Gets cardinality of dataset row (maximum number of the elements).
     *
     * @return This dataset row's cardinality.
     */
    public int size() {
        return vector.size();
    }

    /**
     * Gets the value at specified index.
     *
     * @param idx DatasetRow index.
     * @return DatasetRow value.
     * @throws IndexException Throw if index is out of bounds.
     */
    public double get(int idx) {
        return vector.get(idx);
    }

    /**
     * Gets the value at specified index.
     *
     * @param idx DatasetRow index.
     * @return DatasetRow value.
     * @throws IndexException Throw if index is out of bounds.
     */
    public Serializable getRaw(int idx) {
        return vector.getRaw(idx);
    }

    /**
     * Sets value.
     *
     * @param idx Dataset row index to set value at.
     * @param val Value to set.
     * @throws IndexException Throw if index is out of bounds.
     */
    public void set(int idx, double val) {
        vector.set(idx, val);
    }
}
