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

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.ml.knn.models.Normalization;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.apache.ignite.ml.math.exceptions.NoDataException;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.exceptions.knn.EmptyFileException;
import org.apache.ignite.ml.math.exceptions.knn.FileParsingException;
import org.apache.ignite.ml.math.exceptions.knn.NoLabelVectorException;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.math.impls.vector.SparseBlockDistributedVector;
import org.jetbrains.annotations.NotNull;

/**
 * Class for set of labeled vectors.
 */
public class LabeledDataset implements Serializable {
    /** Data to keep. */
    private final LabeledVector[] data;

    /** Feature names (one name for each attribute in vector). */
    private String[] featureNames;

    /** Amount of instances. */
    private int rowSize;

    /** Amount of attributes in each vector. */
    private int colSize;

    /**
     * Creates new Labeled Dataset by given data.
     *
     * @param data Should be initialized with one vector at least.
     * @param colSize Amount of observed attributes in each vector.
     */
    public LabeledDataset(LabeledVector[] data, int colSize) {
        this(data, null, colSize);
    }

    /**
     * Creates new Labeled Dataset by given data.
     *
     * @param data Given data. Should be initialized with one vector at least.
     * @param featureNames Column names.
     * @param colSize Amount of observed attributes in each vector.
     */
    public LabeledDataset(LabeledVector[] data, String[] featureNames, int colSize) {
        assert data != null;
        assert data.length > 0;

        this.data = data;
        this.rowSize = data.length;
        this.colSize = colSize;

        if(featureNames == null) generateFeatureNames();
        else {
            assert colSize == featureNames.length;
            this.featureNames = featureNames;
        }

    }

    /**
     * Creates new Labeled Dataset and initialized with empty data structure.
     *
     * @param rowSize Amount of instances. Should be > 0.
     * @param colSize Amount of attributes. Should be > 0.
     * @param isDistributed Use distributed data structures to keep data.
     */
    public LabeledDataset(int rowSize, int colSize,  boolean isDistributed){
        this(rowSize, colSize, null, isDistributed);
    }

    /**
     * Creates new local Labeled Dataset and initialized with empty data structure.
     *
     * @param rowSize Amount of instances. Should be > 0.
     * @param colSize Amount of attributes. Should be > 0.
     */
    public LabeledDataset(int rowSize, int colSize){
        this(rowSize, colSize, null, false);
    }

    /**
     * Creates new Labeled Dataset and initialized with empty data structure.
     *
     * @param rowSize Amount of instances. Should be > 0.
     * @param colSize Amount of attributes. Should be > 0
     * @param featureNames Column names.
     * @param isDistributed Use distributed data structures to keep data.
     */
    public LabeledDataset(int rowSize, int colSize, String[] featureNames, boolean isDistributed){
        assert rowSize > 0;
        assert colSize > 0;

        if(featureNames == null) generateFeatureNames();
        else {
            assert colSize == featureNames.length;
            this.featureNames = featureNames;
        }

        this.rowSize = rowSize;
        this.colSize = colSize;

        data = new LabeledVector[rowSize];
        for (int i = 0; i < rowSize; i++)
            data[i] = new LabeledVector<>(getVector(colSize, isDistributed), null);

    }


    /**
     * Creates new local Labeled Dataset by matrix and vector of labels.
     *
     * @param mtx Given matrix with rows as observations.
     * @param lbs Labels of observations.
     */
    public LabeledDataset(double[][] mtx, double[] lbs) {
       this(mtx, lbs, null, false);
    }

    /**
     * Creates new Labeled Dataset by matrix and vector of labels.
     *
     * @param mtx Given matrix with rows as observations.
     * @param lbs Labels of observations.
     * @param featureNames Column names.
     * @param isDistributed Use distributed data structures to keep data.
     */
    public LabeledDataset(double[][] mtx, double[] lbs, String[] featureNames, boolean isDistributed) {
        assert mtx != null;
        assert lbs != null;

        if(mtx.length != lbs.length)
            throw new CardinalityException(lbs.length, mtx.length);

        if(mtx[0] == null)
            throw new NoDataException("Pass filled array, the first vector is empty");

        this.rowSize = lbs.length;
        this.colSize = mtx[0].length;

        if(featureNames == null) generateFeatureNames();
        else this.featureNames = featureNames;


        data = new LabeledVector[rowSize];
        for (int i = 0; i < rowSize; i++){

            data[i] = new LabeledVector<>(getVector(colSize, isDistributed), lbs[i]);
            for (int j = 0; j < colSize; j++) {
                try {
                    data[i].features().set(j, mtx[i][j]);
                } catch (ArrayIndexOutOfBoundsException e) {
                    throw new NoDataException("No data in given matrix by coordinates (" + i + "," + j + ")");
                }
            }
        }
    }

    /** */
    private void generateFeatureNames() {
        featureNames = new String[colSize];

        for (int i = 0; i < colSize; i++)
            featureNames[i] = "f_" + i;
    }


    /**
     * Get vectors and their labels.
     *
     * @return Array of Label Vector instances.
     */
    public LabeledVector[] data() {
        return data;
    }

    /**
     * Gets amount of observation.
     *
     * @return Amount of rows in dataset.
     */
    public int rowSize(){
        return rowSize;
    }

    /**
     * Returns feature name for column with given index.
     *
     * @param i The given index.
     * @return Feature name.
     */
    public String getFeatureName(int i){
        return featureNames[i];
    }

    /**
     * Gets amount of attributes.
     *
     * @return Amount of attributes in each Labeled Vector.
     */
    public int colSize(){
        return colSize;
    }

    /**
     * Retrieves Labeled Vector by given index.
     *
     * @param idx Index of observation.
     * @return Labeled features.
     */
    public LabeledVector getRow(int idx){
        return data[idx];
    }

    /**
     * Get the features.
     *
     * @param idx Index of observation.
     * @return Vector with features.
     */
    public Vector features(int idx){
        assert idx < rowSize;
        assert data != null;
        assert data[idx] != null;

        return data[idx].features();
    }

    /**
     * Returns label if label is attached or null if label is missed.
     *
     * @param idx Index of observation.
     * @return Label.
     */
    public double label(int idx) {
        LabeledVector labeledVector = data[idx];

        if(labeledVector!=null)
            return (double)labeledVector.label();
        else
            return Double.NaN;
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
        LabeledVector labeledVector = data[idx];

        if(labeledVector != null)
            labeledVector.setLabel(lb);
        else
            throw new NoLabelVectorException(idx);
    }

    /**
     * Datafile should keep class labels in the first column.
     *
     * @param pathToFile Path to file.
     * @param separator Element to tokenize row on separate tokens.
     * @param isDistributed Generates distributed dataset if true.
     * @param isFallOnBadData Fall on incorrect data if true.
     * @return Labeled Dataset parsed from file.
     */
    public static LabeledDataset loadTxt(Path pathToFile, String separator, boolean isDistributed, boolean isFallOnBadData) throws IOException {
        Stream<String> stream = Files.lines(pathToFile);
        List<String> list = new ArrayList<>();
        stream.forEach(list::add);

        final int rowSize = list.size();

        List<Double> labels = new ArrayList<>();
        List<Vector> vectors = new ArrayList<>();

        if (rowSize > 0) {

            final int colSize = getColumnSize(separator, list) - 1;

            if (colSize > 0) {

                for (int i = 0; i < rowSize; i++) {
                    Double clsLb;

                    String[] rowData = list.get(i).split(separator);

                    try {
                        clsLb = Double.parseDouble(rowData[0]);
                        Vector vec = parseFeatures(pathToFile, isDistributed, isFallOnBadData, colSize, i, rowData);
                        labels.add(clsLb);
                        vectors.add(vec);
                    }
                    catch (NumberFormatException e) {
                        if(isFallOnBadData)
                            throw new FileParsingException(rowData[0], i, pathToFile);
                    }
                }

                LabeledVector[] data = new LabeledVector[vectors.size()];
                for (int i = 0; i < vectors.size(); i++)
                    data[i] = new LabeledVector<>(vectors.get(i), labels.get(i));

                return new LabeledDataset(data, colSize);
            }
            else
                throw new NoDataException("File should contain first row with data");
        }
        else
            throw new EmptyFileException(pathToFile.toString());
    }

    /** */
    @NotNull private static Vector parseFeatures(Path pathToFile, boolean isDistributed, boolean isFallOnBadData,
        int colSize, int rowIdx, String[] rowData) {
        final Vector vec = getVector(colSize, isDistributed);

        for (int j = 0; j < colSize; j++) {

            if (rowData.length == colSize + 1) {
                double val = fillMissedData();

                try {
                    val = Double.parseDouble(rowData[j + 1]);
                    vec.set(j, val);
                }
                catch (NumberFormatException e) {
                    if(isFallOnBadData)
                        throw new FileParsingException(rowData[j + 1], rowIdx, pathToFile);
                    else
                        vec.set(j,val);
                }
            }
            else throw new CardinalityException(colSize + 1, rowData.length);
        }
        return vec;
    }

    // TODO: IGNITE-7025 add filling with mean, mode, ignoring and so on
    /** */
    private static double fillMissedData() {
            return 0.0;
    }

    /** */
    @NotNull private static Vector getVector(int size, boolean isDistributed) {

        if(isDistributed) return new SparseBlockDistributedVector(size);
        else return new DenseLocalOnHeapVector(size);
    }

    /** */
    private static int getColumnSize(String separator, List<String> list) {
        String[] rowData = list.get(0).split(separator, -1); // assume that all observation has the same length as a first row

        return rowData.length;
    }

    /**
     * Scales features in dataset.
     *
     * @param normalization normalization approach
     * @return Labeled dataset
     */
    public LabeledDataset normalizeWith(Normalization normalization) {
        switch (normalization){
            case MINIMAX: minMaxFeatures();
                break;
            case Z_NORMALIZATION: throw new UnsupportedOperationException("Z-normalization is not supported yet");
        }

        return this;
    }

    /**
     * Complexity 2*N^2. Try to optimize.
     */
    private void minMaxFeatures() {
        double[] mins = new double[colSize];
        double[] maxs = new double[colSize];

        for (int j = 0; j < colSize; j++) {
            double maxInCurrCol = Double.MIN_VALUE;
            double minInCurrCol = Double.MAX_VALUE;

            for (int i = 0; i < rowSize; i++) {
                double e = data[i].features().get(j);
                maxInCurrCol = Math.max(e, maxInCurrCol);
                minInCurrCol = Math.min(e, minInCurrCol);
            }

            mins[j] = minInCurrCol;
            maxs[j] = maxInCurrCol;
        }

        for (int j = 0; j < colSize; j++) {
            double div = maxs[j] - mins[j];

            for (int i = 0; i < rowSize; i++) {
                double oldVal = data[i].features().get(j);
                double newVal = (oldVal - mins[j])/div;
                // x'=(x-MIN[X])/(MAX[X]-MIN[X])
                data[i].features().set(j, newVal);
            }
        }
    }

    /** */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        LabeledDataset that = (LabeledDataset)o;

        return rowSize == that.rowSize && colSize == that.colSize && Arrays.equals(data, that.data) && Arrays.equals(featureNames, that.featureNames);
    }

    /** */
    @Override public int hashCode() {
        int res = Arrays.hashCode(data);
        res = 31 * res + Arrays.hashCode(featureNames);
        res = 31 * res + rowSize;
        res = 31 * res + colSize;
        return res;
    }
}
