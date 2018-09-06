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
import java.util.Arrays;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Class for set of vectors. This is a base class in hierarchy of datasets.
 */
public class Dataset<Row extends DatasetRow> implements Serializable, Externalizable {
    /** Data to keep. */
    protected Row[] data;

    /** Metadata to identify feature. */
    protected FeatureMetadata[] meta;

    /** Amount of instances. */
    protected int rowSize;

    /** Amount of attributes in each vector. */
    protected int colSize;

    /**  */
    protected boolean isDistributed;

    /**
     * Default constructor (required by Externalizable).
     */
    public Dataset(){}

    /**
     * Creates new Dataset by given data.
     *
     * @param data Given data. Should be initialized with one vector at least.
     * @param meta Feature's metadata.
     */
    public Dataset(Row[] data, FeatureMetadata[] meta) {
        this.data = data;
        this.meta = meta;
    }

    /**
     * Creates new Dataset by given data.
     *
     * @param data Given data. Should be initialized with one vector at least.
     * @param featureNames Column names.
     * @param colSize Amount of observed attributes in each vector.
     */
    public Dataset(Row[] data, String[] featureNames, int colSize) {
        this(data.length, colSize, featureNames, false);

        assert data != null;

        this.data = data;
    }

    /**
     * Creates new Dataset by given data.
     *
     * @param data Should be initialized with one vector at least.
     * @param colSize Amount of observed attributes in each vector.
     */
    public Dataset(Row[] data, int colSize) {
        this(data, null, colSize);
    }

    /**
     * Creates new Dataset by given data.
     *
     * @param data Should be initialized with one vector at least.
     */
    public Dataset(Row[] data) {
        this.data = data;
        this.rowSize = data.length;
    }

    /**
     * Creates new Dataset and initialized with empty data structure.
     *
     * @param rowSize Amount of instances. Should be > 0.
     * @param colSize Amount of attributes. Should be > 0
     * @param featureNames Column names.
     */
    public Dataset(int rowSize, int colSize, String[] featureNames, boolean isDistributed) {
        assert rowSize > 0;
        assert colSize > 0;

        if (featureNames == null)
            generateFeatureNames();
        else {
            assert colSize == featureNames.length;
            convertStringNamesToFeatureMetadata(featureNames);
        }

        this.rowSize = rowSize;
        this.colSize = colSize;
        this.isDistributed = isDistributed;
    }

    /** */
    protected void convertStringNamesToFeatureMetadata(String[] featureNames) {
        this.meta = new FeatureMetadata[featureNames.length];
        for (int i = 0; i < featureNames.length; i++)
            this.meta[i] = new FeatureMetadata(featureNames[i]);
    }

    /** */
    protected void generateFeatureNames() {
        String[] featureNames = new String[colSize];

        for (int i = 0; i < colSize; i++)
            featureNames[i] = "f_" + i;

        convertStringNamesToFeatureMetadata(featureNames);
    }

    /**
     * Returns feature name for column with given index.
     *
     * @param i The given index.
     * @return Feature name.
     */
    public String getFeatureName(int i) {
        return meta[i].name();
    }

    /** */
    public DatasetRow[] data() {
        return data;
    }

    /** */
    public void setData(Row[] data) {
        this.data = data;
    }

    /** */
    public FeatureMetadata[] meta() {
        return meta;
    }

    /** */
    public void setMeta(FeatureMetadata[] meta) {
        this.meta = meta;
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

    /** */
    public boolean isDistributed() {
        return isDistributed;
    }

    /** */
    public void setDistributed(boolean distributed) {
        isDistributed = distributed;
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

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        Dataset that = (Dataset)o;

        return rowSize == that.rowSize && colSize == that.colSize && Arrays.equals(data, that.data) && Arrays.equals(meta, that.meta);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = Arrays.hashCode(data);
        res = 31 * res + Arrays.hashCode(meta);
        res = 31 * res + rowSize;
        res = 31 * res + colSize;
        res = 31 * res + (isDistributed ? 1 : 0);
        return res;
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(data);
        out.writeObject(meta);
        out.writeInt(rowSize);
        out.writeInt(colSize);
        out.writeBoolean(isDistributed);
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        data = (Row[]) in.readObject();
        meta = (FeatureMetadata[]) in.readObject();
        rowSize = in.readInt();
        colSize = in.readInt();
        isDistributed = in.readBoolean();
    }
}
