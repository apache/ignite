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

package org.apache.ignite.ml.structures.newstructures;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.structures.DatasetRow;
import org.apache.ignite.ml.structures.FeatureMetadata;

/**
 * Class for set of vectors. This is a base class in hierarchy of datasets.
 */
public class NewDataset<Row extends DatasetRow> implements Serializable, Externalizable {
    /** Data to keep. */
    protected Row[] data;

    /** Amount of instances. */
    protected int rowSize;

    /** Amount of attributes in each vector. */
    protected int colSize;

    /**
     * Default constructor (required by Externalizable).
     */
    public NewDataset(){}



    /** */
    public DatasetRow[] data() {
        return data;
    }

    /** */
    public void setData(Row[] data) {
        this.data = data;
    }

    /**
     * Gets amount of attributes.
     *
     * @return Amount of attributes in each Labeled Vector.
     */
    public int colSize() {
        return colSize;
    }

    /**
     * Gets amount of observation.
     *
     * @return Amount of rows in dataset.
     */
    public int rowSize() {
        return rowSize;
    }

    /**
     * Retrieves Labeled Vector by given index.
     *
     * @param idx Index of observation.
     * @return Labeled features.
     */
    public Row getRow(int idx) {
        return data[idx];
    }

    /**
     * Get the features.
     *
     * @param idx Index of observation.
     * @return Vector with features.
     */
    public Vector features(int idx) {
        assert idx < rowSize;
        assert data != null;
        assert data[idx] != null;

        return data[idx].features();
    }

/*    *//** {@inheritDoc} *//*
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        NewDataset that = (NewDataset)o;

        return rowSize == that.rowSize && colSize == that.colSize && Arrays.equals(data, that.data) && Arrays.equals(meta, that.meta);
    }*/

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = Arrays.hashCode(data);
      /*  res = 31 * res + Arrays.hashCode(meta);
        res = 31 * res + rowSize;
        res = 31 * res + colSize;
        res = 31 * res + (isDistributed ? 1 : 0);*/
        return res;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(data);
/*        out.writeObject(meta);
        out.writeInt(rowSize);
        out.writeInt(colSize);
        out.writeBoolean(isDistributed);*/
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        data = (Row[]) in.readObject();
/*        meta = (FeatureMetadata[]) in.readObject();
        rowSize = in.readInt();
        colSize = in.readInt();
        isDistributed = in.readBoolean();*/
    }
}
