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
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.newstructures.NewLabeledDataset;
import org.jetbrains.annotations.NotNull;

/**
 * kNN algorithm is a classification algorithm.
 */
public class NewKNNModel implements Model<Vector, Double>, Exportable<KNNModelFormat> {
    /** Amount of nearest neighbors. */
    protected int k = 5;

    /** Distance measure. */
    protected DistanceMeasure distanceMeasure = new EuclideanDistance();

    /** Training dataset. */
    protected NewLabeledDataset training;

    /** kNN strategy. */
    protected KNNStrategy stgy = KNNStrategy.SIMPLE;

    /** Cached distances for k-nearest neighbors. */
    protected double[] cachedDistances;

    public NewKNNModel(NewLabeledDataset data) {
        this.training = data;
    }

    /** {@inheritDoc} */
    @Override public Double apply(Vector v) {
        LabeledVector[] neighbors = findKNearestNeighbors(v, true);

        return classify(neighbors, v, stgy);
    }

    /** */
    @Override public <P> void saveModel(Exporter<KNNModelFormat, P> exporter, P path) {
  /*      KNNModelFormat mdlData = new KNNModelFormat(k, distanceMeasure, training, stgy);

        exporter.save(mdlData, path);*/
    }

    /**
     * The main idea is calculation all distance pairs between given vector and all vectors in training set, sorting
     * them and finding k vectors with min distance with the given vector.
     *
     * @param v The given vector.
     * @return K-nearest neighbors.
     */
    protected LabeledVector[] findKNearestNeighbors(Vector v, boolean isCashedDistance) {
        LabeledVector[] trainingData = (LabeledVector[])training.data();

        TreeMap<Double, Set<Integer>> distanceIdxPairs = getDistances(v, trainingData);

        return getKClosestVectors(trainingData, distanceIdxPairs, isCashedDistance);
    }

    /**
     * Iterates along entries in distance map and fill the resulting k-element array.
     *
     * @param trainingData The training data.
     * @param distanceIdxPairs The distance map.
     * @param isCashedDistances Cache distances if true.
     * @return K-nearest neighbors.
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
     * Computes distances between given vector and each vector in training dataset.
     *
     * @param v The given vector.
     * @param trainingData The training dataset.
     * @return Key - distanceMeasure from given features before features with idx stored in value. Value is presented
     * with Set because there can be a few vectors with the same distance.
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
            double clsLb = (double)neighbor.label();

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

        NewKNNModel that = (NewKNNModel)obj;

        return k == that.k && distanceMeasure.equals(that.distanceMeasure) && stgy.equals(that.stgy)
            && Arrays.deepEquals(training.data(), that.training.data());
    }

    public NewKNNModel withK(int k) {
        this.k = k;
        return this;
    }

    public NewKNNModel withStrategy(KNNStrategy stgy) {
        this.stgy = stgy;
        return this;
    }

    public NewKNNModel withDistanceMeasure(DistanceMeasure distanceMeasure) {
        this.distanceMeasure = distanceMeasure;
        return this;
    }
}
