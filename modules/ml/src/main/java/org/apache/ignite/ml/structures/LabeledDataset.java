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

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.impls.matrix.SparseBlockDistributedMatrix;
import org.apache.ignite.ml.math.impls.vector.SparseBlockDistributedVector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Class for matrix with label.
 *
 * @param <M> Some class extending {@link Vector}.
 * @param <L> Type of label.
 */
public class LabeledDataset<M extends Matrix, L extends Vector> {
    /** Vector. */
    private final M matrix;

    /** Label. */
    private final L lbs;

    /**
     * Construct labeled dataset.
     *
     * @param matrix Matrix.
     * @param lbs Labels.
     */
    public LabeledDataset(M matrix, L lbs) {
        this.matrix = matrix;
        this.lbs = lbs;
    }

    /**
     * Get the vector.
     *
     * @return Vector.
     */
    public M data() {
        return matrix;
    }

    /**
     * Get the label.
     *
     * @return Label.
     */
    public L labels() {
        return lbs;
    }

    public int columnSize(){
        return matrix.columnSize();
    }

    public int rowSize(){
        return matrix.rowSize();
    }

    /**
     * Retrieves Labeled Vector by given index
     * @param idx index of observation
     * @return Labeled vector
     */
    public LabeledVector getRow(int idx){
        return new LabeledVector(matrix.getRow(idx), lbs.get(idx));
    }


    public double label(int idx) {
        return lbs.get(idx);
    }
    /**
     * Datafile should keep class labels in the first column
     * @param pathToFile
     * @param separator
     * @return
     */
    public static LabeledDataset<Matrix, Vector> loadTxt(String pathToFile, String separator) {


        Matrix mtx;
        Vector labels;

        try (Stream<String> stream = Files.lines(Paths.get(pathToFile))) {
            List<String> list = new ArrayList<>();
            stream.forEach(list::add);

            final int rowSize = list.size();

            if(rowSize > 0){
                final int columnSize = getColumnSize(separator, list) - 1;

                mtx = new SparseBlockDistributedMatrix(rowSize, columnSize);
                labels = new SparseBlockDistributedVector(rowSize);
                for (int i = 0; i < rowSize; i++) {
                    String[] rowData = list.get(i).split(separator);

                    try {
                        double classLabel = Double.parseDouble(rowData[0]);
                        labels.set(i, classLabel);
                    } catch(NumberFormatException e) {
                        // log or something else throw unparsed class label
                        // mtx.set(i,j,0.0); should we set 0 or not?
                    }

                    for (int j = 0; j < columnSize; j++) {
                        
                        if(rowData.length == columnSize + 1){
                            double value = 0.0;
                            try {
                                 value = Double.parseDouble(rowData[j+1]);
                                mtx.set(i,j,value);
                            } catch(NumberFormatException e) {
                                // log or something else
                                // mtx.set(i,j,0.0); should we set 0 or not?
                            }
                        } else {
                            // throw Strange Size
                        }
                    }
                }

                return new LabeledDataset<>(mtx, labels);

            } else {
                // throw Exception : 0 rows were parsed, empty dataset
            }



        } catch (IOException e) {
            e.printStackTrace();
            // throw and log
        }
        return null;
    }

    private static int getColumnSize(String separator, List<String> list) {
        String[] rowData = list.get(0).split(separator, -1); // assume that all observation has the same length as a first row

        return rowData.length;
    }

}
