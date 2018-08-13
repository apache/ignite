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

package org.apache.ignite.ml.structures.preprocessing;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.apache.ignite.ml.math.exceptions.NoDataException;
import org.apache.ignite.ml.math.exceptions.knn.EmptyFileException;
import org.apache.ignite.ml.math.exceptions.knn.FileParsingException;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;
import org.jetbrains.annotations.NotNull;

/** Data pre-processing step which loads data from different file types. */
public class LabeledDatasetLoader {
    /**
     * Datafile should keep class labels in the first column.
     *
     * @param pathToFile Path to file.
     * @param separator Element to tokenize row on separate tokens.
     * @param isDistributed Generates distributed dataset if true.
     * @param isFallOnBadData Fall on incorrect data if true.
     * @return Labeled Dataset parsed from file.
     */
    public static LabeledDataset loadFromTxtFile(Path pathToFile, String separator, boolean isDistributed,
        boolean isFallOnBadData) throws IOException {
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
                        if (isFallOnBadData)
                            throw new FileParsingException(rowData[0], i, pathToFile);
                    }
                }

                LabeledVector[] data = new LabeledVector[vectors.size()];
                for (int i = 0; i < vectors.size(); i++)
                    data[i] = new LabeledVector(vectors.get(i), labels.get(i));

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
        final Vector vec = LabeledDataset.emptyVector(colSize, isDistributed);

        if (isFallOnBadData && rowData.length != colSize + 1)
            throw new CardinalityException(colSize + 1, rowData.length);

        double missedData = fillMissedData();

        for (int j = 0; j < colSize; j++) {
            try {
                double feature = Double.parseDouble(rowData[j + 1]);
                vec.set(j, feature);
            }
            catch (NumberFormatException e) {
                if (isFallOnBadData)
                    throw new FileParsingException(rowData[j + 1], rowIdx, pathToFile);
                else
                    vec.set(j, missedData);
            }
            catch (ArrayIndexOutOfBoundsException e){
                vec.set(j, missedData);
            }
        }
        return vec;
    }

    // TODO: IGNITE-7025 add filling with mean, mode, ignoring and so on

    /** */
    private static double fillMissedData() {
        return 0.0;
    }

    /** */
    private static int getColumnSize(String separator, List<String> list) {
        String[] rowData = list.get(0).split(separator, -1); // assume that all observation has the same length as a first row

        return rowData.length;
    }
}
