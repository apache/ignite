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

import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;

import java.util.*;

/**
 * Model for kNN.
 */
public class KNNModel implements Model<Vector, Double>, Exportable<KNNModelFormat> {
    /** Amount of nearest neighbors */
    private final int k;

    /** Distance measure */
    private final DistanceMeasure distanceMeasure;

    /** Training dataset */
    private final LabeledDataset<Matrix, Vector>  training;

    /** kNN strategy */
    private final KNNStrategy strategy;

    /**
     *
     * @param k amount of nearest neighbors
     * @param training
     */
    public KNNModel(int k, DistanceMeasure distanceMeasure, KNNStrategy strategy, LabeledDataset<Matrix, Vector> training) {
        this.k = k;
        this.distanceMeasure = distanceMeasure;
        this.training = training;
        this.strategy = strategy;

        // TODO: throw small training size if k > training size
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector v) {

        LabeledVector[] neighbors = findKNearestNeighbors(v);
        double classLabel = classify(neighbors, v,  strategy);

        return classLabel;
    }

    @Override
    public <P> void saveModel(Exporter<KNNModelFormat, P> exporter, P path) {

    }

    private LabeledVector[] findKNearestNeighbors(Vector v){
        LabeledVector[] res = new LabeledVector[k];
        Matrix trainingData = training.data();

        // key - distanceMeasure from given vector before vector with idx stored in value
        // value is presented with Set because there can be a few vectors with the same distance
        Map<Double, Set<Integer>> distanceIdxPairs = new TreeMap<>();

        for (int i = 0; i < trainingData.rowSize(); i++) {

            double distance = distanceMeasure.compute(v, trainingData.getRow(i));
            putDistanceIdxPair(distanceIdxPairs, i, distance);

        }

        int i = 0;
        while(i < k) {
            double key = distanceIdxPairs.keySet().iterator().next();
            Set<Integer> idxs = distanceIdxPairs.get(key);
            for (Integer idx : idxs){
                res[i] = new LabeledVector(trainingData.getRow(idx), training.labels().get(idx));  // TODO: refactor LV and LD communication
                i++;
                if(i >= k) break; // go to next while-loop iteration
            }
        }

        return res;
    }

    private void putDistanceIdxPair(Map<Double, Set<Integer>> distanceIdxPairs, int i, double distance) {
        if(distanceIdxPairs.containsKey(distance)){
            Set<Integer> idxs = distanceIdxPairs.get(distance);
            idxs.add(i);
        } else {
            Set<Integer> idxs = new HashSet<>();
            idxs.add(i);
            distanceIdxPairs.put(distance, idxs);
        }
    }

    private double classify(LabeledVector[] neighbors, Vector v, KNNStrategy strategy){

        TreeMap<Double, Double> classVotes = new TreeMap<>();
        for (int i = 0; i < neighbors.length; i++) {
            LabeledVector neighbor = neighbors[i];
            double classLabel = (double) neighbor.label(); // TODO: handle casting correctly and for different types

            double distance = distanceMeasure.compute(v, neighbor.vector()); // TODO: repeated calculation
            if(classVotes.containsKey(classLabel)){
                double classVote = classVotes.get(classLabel);
                classVote += getClassVoteForVector(strategy, distance);
                classVotes.put(classLabel, classVote);
            } else {
                final double value = getClassVoteForVector(strategy, distance);
                classVotes.put(classLabel, value);
            }
        }

        return classVotes.lastKey();
    }


    // TODO: handle different strategies
    private double getClassVoteForVector(KNNStrategy strategy, double distance) {

        if (strategy.equals(strategy.WEIGHTED))
            return 1/distance;
        else  return 1.0; // strategy.SIMPLE

    }
}
