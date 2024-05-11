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

import java.io.Serializable;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.TreeSet;
import org.jetbrains.annotations.NotNull;

/**
 * Class for splitting Labeled Dataset on train and test sets.
 */
public class LabeledVectorSetTestTrainPair implements Serializable {
    /** Data to keep train set. */
    private LabeledVectorSet train;

    /** Data to keep test set. */
    private LabeledVectorSet test;

    /**
     * Creates two subsets of given dataset.
     * <p>
     * NOTE: This method uses next algorithm with O(n log n) by calculations and O(n) by memory.
     * </p>
     * @param dataset The dataset to split on train and test subsets.
     * @param testPercentage The percentage of the test subset.
     */
    public LabeledVectorSetTestTrainPair(LabeledVectorSet dataset, double testPercentage) {
        assert testPercentage > 0.0;
        assert testPercentage < 1.0;
        final int datasetSize = dataset.rowSize();
        assert datasetSize > 2;

        final int testSize = (int)Math.floor(datasetSize * testPercentage);
        final int trainSize = datasetSize - testSize;

        final TreeSet<Integer> sortedTestIndices = getSortedIndices(datasetSize, testSize);

        LabeledVector[] testVectors = new LabeledVector[testSize];
        LabeledVector[] trainVectors = new LabeledVector[trainSize];

        int datasetCntr = 0;
        int trainCntr = 0;
        int testCntr = 0;

        for (Integer idx: sortedTestIndices) { // guarantee order as iterator
            testVectors[testCntr] = (LabeledVector)dataset.getRow(idx);
            testCntr++;

            for (int i = datasetCntr; i < idx; i++) {
                trainVectors[trainCntr] = (LabeledVector)dataset.getRow(i);
                trainCntr++;
            }

            datasetCntr = idx + 1;
        }
        if (datasetCntr < datasetSize) {
            for (int i = datasetCntr; i < datasetSize; i++) {
                trainVectors[trainCntr] = (LabeledVector)dataset.getRow(i);
                trainCntr++;
            }
        }

        test = new LabeledVectorSet(testVectors, dataset.colSize());
        train = new LabeledVectorSet(trainVectors, dataset.colSize());
    }

    /** This method generates "random double, integer" pairs, sort them, gets first "testSize" elements and returns appropriate indices */
    @NotNull private TreeSet<Integer> getSortedIndices(int datasetSize, int testSize) {
        Random rnd = new Random();
        TreeMap<Double, Integer> randomIdxPairs = new TreeMap<>();
        for (int i = 0; i < datasetSize; i++)
            randomIdxPairs.put(rnd.nextDouble(), i);

        final TreeMap<Double, Integer> testIdxPairs = randomIdxPairs.entrySet().stream()
            .limit(testSize)
            .collect(TreeMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), Map::putAll);

        return new TreeSet<>(testIdxPairs.values());
    }

    /**
     * Train subset of the whole dataset.
     * @return Train subset.
     */
    public LabeledVectorSet train() {
        return train;
    }

    /**
     * Test subset of the whole dataset.
     * @return Test subset.
     */
    public LabeledVectorSet test() {
        return test;
    }
}
