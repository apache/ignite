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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.ml.knn.models.FillMissingValueWith;
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
 * Class for set of labeled vectors
 */
public class LabeledDataset {
    /** Data to keep */
    private final LabeledVector<Vector, Double>[] data;

    /** Feature names (one name for each attribute in vector) */
    private String[] featureNames;

    /** Amount of instances */
    private int rowSize;

    /** Amount of attributes in each vector */
    private int colSize;

    /**
     * Creates new Labeled Dataset by given data
     * @param data Should be initialized with one vector at least
     * @param colSize amount of observed attributes in each vector
     */
    public LabeledDataset(LabeledVector[] data, int colSize) {
        this(data, null, colSize);
    }

    /**
     * Creates new Labeled Dataset by given data
     * @param data Given data. Should be initialized with one vector at least
     * @param featureNames Column names
     * @param colSize Amount of observed attributes in each vector
     */
    public LabeledDataset(LabeledVector[] data, String[] featureNames, int colSize) {
        assert data != null;
        assert data.length > 0;

        if(featureNames == null) generateFeatureNames();
        else assert data.length == featureNames.length;

        this.data = data;
        this.rowSize = data.length;
        this.colSize = colSize;
        this.featureNames = featureNames;

    }

    /**
     * Creates new Labeled Dataset and initialized with empty data structure
     * @param rowSize Amount of instances. Should be > 0
     * @param colSize Amount of attributes. Should be > 0
     * @param isDistributed Use distributed data structures to keep data
     */
    public LabeledDataset(int rowSize, int colSize,  boolean isDistributed){
        this(rowSize, colSize, null, isDistributed);
    }

    /**
     * Creates new local Labeled Dataset and initialized with empty data structure
     * @param rowSize Amount of instances. Should be > 0
     * @param colSize Amount of attributes. Should be > 0
     */
    public LabeledDataset(int rowSize, int colSize){
        this(rowSize, colSize, null, false);
    }

    /**
     * Creates new Labeled Dataset and initialized with empty data structure
     * @param rowSize Amount of instances. Should be > 0
     * @param colSize Amount of attributes. Should be > 0
     * @param featureNames Column names
     * @param isDistributed Use distributed data structures to keep data
     */
    public LabeledDataset(int rowSize, int colSize, String[] featureNames, boolean isDistributed){
        assert rowSize > 0;
        assert colSize > 0;

        if(featureNames == null) generateFeatureNames();
        else assert colSize == featureNames.length;

        data = new LabeledVector[rowSize];
        for (int i = 0; i < rowSize; i++)
            data[i] = new LabeledVector(getVector(colSize, isDistributed), null);

    }


    /**
     * Creates new local Labeled Dataset by matrix and vector of labels
     * @param mtx Given matrix with rows as observations
     * @param lbs Labels of observations
     */
    public LabeledDataset(double[][] mtx, double[] lbs) {
       this(mtx, lbs, null, false);
    }

    /**
     * Creates new Labeled Dataset by matrix and vector of labels
     * @param mtx Given matrix with rows as observations
     * @param lbs Labels of observations
     * @param featureNames Column names
     * @param isDistributed Use distributed data structures to keep data
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

        data = new LabeledVector[rowSize];
        for (int i = 0; i < rowSize; i++){
            data[i] = new LabeledVector(getVector(colSize, isDistributed), lbs[i]);
            for (int j = 0; j < colSize; j++) {
                try {
                    data[i].features().set(j, mtx[i][j]);
                } catch (ArrayIndexOutOfBoundsException e) {
                    throw new NoDataException("No data in given matrix by coordinates (" + i + "," + j + ")");
                }
            }
        }
    }

    private void generateFeatureNames() {
        featureNames = new String[colSize];
        for (int i = 0; i < colSize; i++)
            featureNames[i] = "f_" + i;
    }


    /**
     * Get vectors and their labels.
     *
     * @return array of Label Vector instances.
     */
    public LabeledVector[] data() {
        return data;
    }

    /**
     * Gets amount of observation
     * @return amount of rows in dataset
     */
    public int rowSize(){
        return rowSize;
    }

    /**
     * Gets amount of attributes
     * @return amount of attributes in each Labeled Vector
     */
    public int colSize(){
        return colSize;
    }

    /**
     * Retrieves Labeled Vector by given index
     * @param idx index of observation
     * @return Labeled features
     */
    public LabeledVector getRow(int idx){
        return data[idx];
    }

    /**
     * Get the features
     * @param idx index of observation
     * @return Vector with features
     */
    public Vector features(int idx){
        assert idx < rowSize;
        assert data != null;
        assert data[idx] != null;

        return data[idx].features();
    }

    /**
     * Returns label if label is attached or null if label is missed
     * @param idx index of observation
     * @return label
     */
    public double label(int idx) {
        LabeledVector labeledVector = data[idx];
        if(labeledVector!=null)
            return (double)labeledVector.label();
        else
            return Double.parseDouble(null);
    }

    /**
     * Fill the label with given value
     * @param idx index of observation
     * @param label given label
     */
    public void setLabel(int idx, double label) {
        LabeledVector labeledVector = data[idx];
        if(labeledVector != null)
            labeledVector.setLabel(label);
        else
            throw new NoLabelVectorException(idx);
    }

    /**
     * Datafile should keep class labels in the first column
     * @param pathToFile Path to file
     * @param separator Element to tokenize row on separate tokens
     * @param isDistributed Generates distributed dataset if true
     * @param isFallOnBadData Fall on incorrect data if true
     * @return Labeled Dataset parsed from file
     */
    public static LabeledDataset loadTxt(Path pathToFile, String separator, boolean isDistributed, boolean isFallOnBadData, FillMissingValueWith fillingStrategy) throws IOException {
        Stream<String> stream = Files.lines(pathToFile);
        List<String> list = new ArrayList<>();
        stream.forEach(list::add);

        final int rowSize = list.size();

        List<Double> labels = new ArrayList<>();
        List<Vector> vectors = new ArrayList<>();

        if (rowSize > 0) {
            final int columnSize = getColumnSize(separator, list) - 1;
            if (columnSize > 0) {
                for (int i = 0; i < rowSize; i++) {
                    Double clsLb;

                    String[] rowData = list.get(i).split(separator);

                    try {
                        clsLb = Double.parseDouble(rowData[0]);
                        Vector vec = parseFeatures(pathToFile, isDistributed, isFallOnBadData, fillingStrategy, columnSize, i, rowData);
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
                    data[i] = new LabeledVector(vectors.get(i), labels.get(i));

                return new LabeledDataset(data, columnSize);
            }
            else
                throw new NoDataException("File should contain first row with data");
        }
        else
            throw new EmptyFileException(pathToFile.toString());
    }

    @NotNull private static Vector parseFeatures(Path pathToFile, boolean isDistributed, boolean isFallOnBadData,
        FillMissingValueWith fillingStrategy, int columnSize, int rowIndex, String[] rowData) {
        final Vector vec = getVector(columnSize, isDistributed);

        for (int j = 0; j < columnSize; j++) {

            if (rowData.length == columnSize + 1) {
                double val = fillMissedData(fillingStrategy);
                try {
                    val = Double.parseDouble(rowData[j + 1]);
                    vec.set(j, val);
                }
                catch (NumberFormatException e) {
                    if(isFallOnBadData)
                        throw new FileParsingException(rowData[j + 1], rowIndex, pathToFile);
                    else
                        vec.set(j,val);
                }
            }
            else throw new CardinalityException(columnSize + 1, rowData.length);
        }
        return vec;
    }

    // TODO: add filling with mean, mode, ignoring and so on
    private static double fillMissedData(FillMissingValueWith fillingStrategy) {
        switch (fillingStrategy){
            case ZERO: return 0.0;
            default: throw new UnsupportedOperationException("Filling missing data is not supported for strategy " + fillingStrategy.name());
        }

    }

    @NotNull private static Vector getVector(int size, boolean isDistributed) {

        if(isDistributed) return new SparseBlockDistributedVector(size);
        else return new DenseLocalOnHeapVector(size);
    }

    private static int getColumnSize(String separator, List<String> list) {
        String[] rowData = list.get(0).split(separator, -1); // assume that all observation has the same length as a first row

        return rowData.length;
    }

    /**
     * Scales features in dataset
     * @param normalization
     * @return
     */
    public LabeledDataset normalizeWith(Normalization normalization) {
        // TODO : https://ru.wikipedia.org/wiki/%D0%9C%D0%B5%D1%82%D0%BE%D0%B4_k-%D0%B1%D0%BB%D0%B8%D0%B6%D0%B0%D0%B9%D1%88%D0%B8%D1%85_%D1%81%D0%BE%D1%81%D0%B5%D0%B4%D0%B5%D0%B9
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
            double maxInCurrentColumn = Double.MIN_VALUE;
            double minInCurrentColumn = Double.MAX_VALUE;
            for (int i = 0; i < rowSize; i++) {
                double e = data[i].features().get(j);
                maxInCurrentColumn = Math.max(e, maxInCurrentColumn);
                minInCurrentColumn = Math.min(e, minInCurrentColumn);
            }
            mins[j] = minInCurrentColumn;
            maxs[j] = maxInCurrentColumn;
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
}
