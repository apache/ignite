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

package org.apache.ignite.ml.knn.models;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.exceptions.knn.SmallTrainingDatasetSizeException;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;
import org.jetbrains.annotations.NotNull;

/**
 * kNN algorithm is a classification algorithm.
 */
public class KNNModel implements Model<Vector, Double>, Exportable<KNNModelFormat> {
    /** Amount of nearest neighbors */
    protected final int k;

    /** Distance measure */
    protected final DistanceMeasure distanceMeasure;

    /** Training dataset */
    protected final LabeledDataset training;

    /** kNN strategy */
    protected final KNNStrategy stgy;

    /** Cached distances for k-nearest neighbors */
    protected double[] cachedDistances;

    /**
     * Creates the kNN model with the given parameters
     *
     * @param k amount of nearest neighbors
     * @param distanceMeasure distance measure
     * @param stgy strategy of calculations
     * @param training training dataset
     */
    public KNNModel(int k, DistanceMeasure distanceMeasure, KNNStrategy stgy, LabeledDataset training) {

        assert training != null;

        if (training.rowSize() < k)
            throw new SmallTrainingDatasetSizeException(k, training.rowSize());

        this.k = k;
        this.distanceMeasure = distanceMeasure;
        this.training = training;
        this.stgy = stgy;
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector v) {

        LabeledVector[] neighbors = findKNearestNeighbors(v, true);

        return classify(neighbors, v, stgy);
    }

    /** */
    @Override public <P> void saveModel(Exporter<KNNModelFormat, P> exporter, P path) {
        KNNModelFormat mdlData = new KNNModelFormat(k, distanceMeasure, training, stgy);

        exporter.save(mdlData, path);
    }

    /**
     * The main idea is calculation all distance pairs between given vector and all vectors in training set, sorting
     * them and finding k vectors with min distance with the given vector
     *
     * @param v the given vector
     * @return k nearest neighbors
     */
    protected LabeledVector[] findKNearestNeighbors(Vector v, boolean isCashedDistance) {

        LabeledVector[] trainingData = training.data();

        TreeMap<Double, Set<Integer>> distanceIdxPairs = getDistances(v, trainingData);

        return getKClosestVectors(trainingData, distanceIdxPairs, isCashedDistance);
    }

    /**
     * Iterates along entries in distance map and fill the resulting k-element array
     *
     * @param trainingData the training data
     * @param distanceIdxPairs the distance map
     * @param isCashedDistances cache distances if true
     * @return k nearest neighbors
     */
    @NotNull private LabeledVector[] getKClosestVectors(LabeledVector[] trainingData,
        TreeMap<Double, Set<Integer>> distanceIdxPairs, boolean isCashedDistances) {
        LabeledVector[] res = new LabeledVector[k];
        int i = 0;
        final Iterator<Double> iter = distanceIdxPairs.keySet().iterator();
        while (i < k) {
            double key = iter.next();
            Set<Integer> idxs = distanceIdxPairs.get(key);
            for (Integer idx : idxs) {
                res[i] = trainingData[idx];
                if (isCashedDistances) {
                    if (cachedDistances == null)
                        cachedDistances = new double[k];
                    cachedDistances[i] = key;
                }
                i++;
                if (i >= k)
                    break; // go to next while-loop iteration
            }
        }
        return res;
    }

    /**
     * Computes distances between given vector and each vector in training dataset
     *
     * @param v The given vector
     * @param trainingData The training dataset
     * @return key - distanceMeasure from given features before features with idx stored in value. Value is presented
     * with Set because there can be a few vectors with the same distance
     */
    @NotNull private TreeMap<Double, Set<Integer>> getDistances(Vector v, LabeledVector[] trainingData) {
        TreeMap<Double, Set<Integer>> distanceIdxPairs = new TreeMap<>();

        for (int i = 0; i < trainingData.length; i++) {

            LabeledVector labeledVector = trainingData[i];
            if (labeledVector != null) {
                double distance = distanceMeasure.compute(v, labeledVector.features());
                putDistanceIdxPair(distanceIdxPairs, i, distance);
            }
        }
        return distanceIdxPairs;
    }

    /** */
    private void putDistanceIdxPair(Map<Double, Set<Integer>> distanceIdxPairs, int i, double distance) {
        if (distanceIdxPairs.containsKey(distance)) {
            Set<Integer> idxs = distanceIdxPairs.get(distance);
            idxs.add(i);
        }
        else {
            Set<Integer> idxs = new HashSet<>();
            idxs.add(i);
            distanceIdxPairs.put(distance, idxs);
        }
    }

    /** */
    private double classify(LabeledVector[] neighbors, Vector v, KNNStrategy stgy) {

        Map<Double, Double> clsVotes = new HashMap<>();
        for (int i = 0; i < neighbors.length; i++) {
            LabeledVector neighbor = neighbors[i];
            double clsLb = (double)neighbor.label(); // TODO: handle different types, not double only

            double distance = cachedDistances != null ? cachedDistances[i] : distanceMeasure.compute(v, neighbor.features());

            if (clsVotes.containsKey(clsLb)) {
                double clsVote = clsVotes.get(clsLb);
                clsVote += getClassVoteForVector(stgy, distance);
                clsVotes.put(clsLb, clsVote);
            }
            else {
                final double val = getClassVoteForVector(stgy, distance);
                clsVotes.put(clsLb, val);
            }
        }

        return getClassWithMaxVotes(clsVotes);
    }

    /** */
    private double getClassWithMaxVotes(Map<Double, Double> clsVotes) {
        return Collections.max(clsVotes.entrySet(), Map.Entry.comparingByValue()).getKey();
    }

    /** */
    private double getClassVoteForVector(KNNStrategy stgy, double distance) {

        if (stgy.equals(KNNStrategy.WEIGHTED))
            return 1 / distance; // strategy.WEIGHTED
        else
            return 1.0; // strategy.SIMPLE
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + k;
        res = res * 37 + distanceMeasure.hashCode();
        res = res * 37 + stgy.hashCode();
        res = res * 37 + Arrays.hashCode(training.data());

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        KNNModel that = (KNNModel)obj;

        return k == that.k && distanceMeasure.equals(that.distanceMeasure) && stgy.equals(that.stgy)
            && Arrays.deepEquals(training.data(), that.training.data());
    }
}
