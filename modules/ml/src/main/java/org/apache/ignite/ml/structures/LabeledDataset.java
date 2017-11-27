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
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.CardinalityException;
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
     * Creates new Labeled Dataset and initialized with empty data structure
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

        LabeledVector[] data = new LabeledVector[rowSize];

        if (rowSize > 0) {
            final int columnSize = getColumnSize(separator, list) - 1;
            for (int i = 0; i < rowSize; i++) {
                Double clsLb;
                final Vector vec = getVector(columnSize, isDistributed);

                String[] rowData = list.get(i).split(separator);

                try {
                    clsLb = Double.parseDouble(rowData[0]);

                    for (int j = 0; j < columnSize; j++) {

                        if (rowData.length == columnSize + 1) {
                            double val = fillMissedData(fillingStrategy);
                            try {
                                val = Double.parseDouble(rowData[j + 1]);
                                vec.set(j, val);
                            }
                            catch (NumberFormatException e) {
                                if(isFallOnBadData)
                                    throw new FileParsingException(rowData[j + 1], i, pathToFile);
                                else
                                    vec.set(j,val);
                            }
                        }
                        else throw new CardinalityException(columnSize + 1, rowData.length);
                    }
                    data[i] = new LabeledVectorDouble(vec, clsLb);
                }
                catch (NumberFormatException e) {
                    if(isFallOnBadData)
                        throw new FileParsingException(rowData[0], i, pathToFile);
                    else
                        clsLb = null;
                }
            }
            return new LabeledDataset(data, columnSize);
        }
        else
            throw new EmptyFileException(pathToFile.toString());
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
}
