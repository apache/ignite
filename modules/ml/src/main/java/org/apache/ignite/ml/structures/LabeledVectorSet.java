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

import org.apache.ignite.ml.math.exceptions.datastructures.NoLabelVectorException;
import org.apache.ignite.ml.math.exceptions.math.CardinalityException;
import org.apache.ignite.ml.math.exceptions.math.NoDataException;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;

/**
 * The set of labeled vectors used in local partition calculations.
 */
public class LabeledVectorSet<Row extends LabeledVector> extends Dataset<Row> implements AutoCloseable {
    /**
     * Default constructor (required by Externalizable).
     */
    public LabeledVectorSet() {
    }

    /**
     * Creates new Labeled Dataset and initialized with empty data structure.
     *
     * @param rowSize Amount of instances. Should be > 0.
     * @param colSize Amount of attributes. Should be > 0.
     */
    public LabeledVectorSet(int rowSize, int colSize) {
        this(rowSize, colSize, null);
    }

    /**
     * Creates new Labeled Dataset and initialized with empty data structure.
     *
     * @param rowSize Amount of instances. Should be > 0.
     * @param colSize Amount of attributes. Should be > 0
     * @param featureNames Column names.
     */
    public LabeledVectorSet(int rowSize, int colSize, String[] featureNames) {
        super(rowSize, colSize, featureNames);

        initializeDataWithLabeledVectors();
    }

    /**
     * Creates new Labeled Dataset by given data.
     *
     * @param data Should be initialized with one vector at least.
     */
    public LabeledVectorSet(Row[] data) {
        super(data);
    }

    /** */
    private void initializeDataWithLabeledVectors() {
        data = (Row[])new LabeledVector[rowSize];
        for (int i = 0; i < rowSize; i++)
            data[i] = (Row)new LabeledVector(emptyVector(colSize), null);
    }

    /**
     * Creates new Labeled Dataset by given data.
     *
     * @param data Should be initialized with one vector at least.
     * @param colSize Amount of observed attributes in each vector.
     */
    public LabeledVectorSet(Row[] data, int colSize) {
        super(data, colSize);
    }

    /**
     * Creates new local Labeled Dataset by matrix and vector of labels.
     *
     * @param mtx Given matrix with rows as observations.
     * @param lbs Labels of observations.
     */
    public LabeledVectorSet(double[][] mtx, double[] lbs) {
       this(mtx, lbs, null);
    }

    /**
     * Creates new Labeled Dataset by matrix and vector of labels.
     *
     * @param mtx Given matrix with rows as observations.
     * @param lbs Labels of observations.
     * @param featureNames Column names.
     */
    public LabeledVectorSet(double[][] mtx, double[] lbs, String[] featureNames) {
        assert mtx != null;
        assert lbs != null;

        if (mtx.length != lbs.length)
            throw new CardinalityException(lbs.length, mtx.length);

        if (mtx[0] == null)
            throw new NoDataException("Pass filled array, the first vector is empty");

        this.rowSize = lbs.length;
        this.colSize = mtx[0].length;

        if (featureNames == null)
            generateFeatureNames();
        else {
            assert colSize == featureNames.length;
            convertStringNamesToFeatureMetadata(featureNames);
        }

        data = (Row[])new LabeledVector[rowSize];
        for (int i = 0; i < rowSize; i++) {
            data[i] = (Row)new LabeledVector(emptyVector(colSize), lbs[i]);
            for (int j = 0; j < colSize; j++) {
                try {
                    data[i].features().set(j, mtx[i][j]);
                } catch (ArrayIndexOutOfBoundsException e) {
                    throw new NoDataException("No data in given matrix by coordinates (" + i + "," + j + ")");
                }
            }
        }
    }

    /**
     * Returns label if label is attached or null if label is missed.
     *
     * @param idx Index of observation.
     * @return Label.
     */
    public double label(int idx) {
        LabeledVector labeledVector = data[idx];

        return labeledVector != null ? (double)labeledVector.label() : Double.NaN;
    }

    /**
     * Returns new copy of labels of all labeled vectors NOTE: This method is useful for copying labels from test
     * dataset.
     *
     * @return Copy of labels.
     */
    public double[] labels() {
        assert data != null;
        assert data.length > 0;

        double[] labels = new double[data.length];

        for (int i = 0; i < data.length; i++)
            labels[i] = (double)data[i].label();

        return labels;
    }

    /**
     * Fill the label with given value.
     *
     * @param idx Index of observation.
     * @param lb The given label.
     */
    public void setLabel(int idx, double lb) {
        LabeledVector<Double> labeledVector = data[idx];

        if (labeledVector != null)
            labeledVector.setLabel(lb);
        else
            throw new NoLabelVectorException(idx);
    }

    /** */
    public static Vector emptyVector(int size) {
            return new DenseVector(size);
    }

    /** Makes copy with new Label objects and old features and Metadata objects. */
    public LabeledVectorSet copy() {
        LabeledVectorSet res = new LabeledVectorSet(this.data, this.colSize);
        res.meta = this.meta;
        for (int i = 0; i < rowSize; i++)
            res.setLabel(i, this.label(i));

        return res;
    }

    /** Closes LabeledDataset. */
    @Override public void close() throws Exception {

    }
}
