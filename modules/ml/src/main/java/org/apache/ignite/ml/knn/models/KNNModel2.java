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
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;

/**
 * Model for kNN.
 */
public class KNNModel2 implements Model<Vector, Double>, Exportable<KNNModelFormat> {
    /** Amount of nearest neighbors */
    protected final int k;

    /** Distance measure */
    protected final DistanceMeasure distanceMeasure;

    /** Training dataset */
    protected final LabeledDataset training;

    /** kNN strategy */
    protected final KNNStrategy strategy;

    /**
     *
     * @param k amount of nearest neighbors
     * @param training
     */
    public KNNModel2(int k, DistanceMeasure distanceMeasure, KNNStrategy strategy, LabeledDataset training) {
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


    // can be default method in model interface or in abstract class
    public void normalizeWith(Normalization normalization){
        // TODO : https://ru.wikipedia.org/wiki/%D0%9C%D0%B5%D1%82%D0%BE%D0%B4_k-%D0%B1%D0%BB%D0%B8%D0%B6%D0%B0%D0%B9%D1%88%D0%B8%D1%85_%D1%81%D0%BE%D1%81%D0%B5%D0%B4%D0%B5%D0%B9
    }

    @Override
    public <P> void saveModel(Exporter<KNNModelFormat, P> exporter, P path) {

    }

    protected LabeledVector[] findKNearestNeighbors(Vector v){
        LabeledVector[] res = new LabeledVector[k];
        LabeledVector[] trainingData = training.data();

        // key - distanceMeasure from given features before features with idx stored in value
        // value is presented with Set because there can be a few vectors with the same distance
        Map<Double, Set<Integer>> distanceIdxPairs = new TreeMap<>();

        for (int i = 0; i < trainingData.length; i++) {

            LabeledVector labeledVector = trainingData[i];
            if(labeledVector != null){
                double distance = distanceMeasure.compute(v, labeledVector.features());
                putDistanceIdxPair(distanceIdxPairs, i, distance);
            }
        }

        int i = 0;
        final Iterator<Double> iterator = distanceIdxPairs.keySet().iterator();
        while(i < k) {
            double key = iterator.next();
            Set<Integer> idxs = distanceIdxPairs.get(key);
            for (Integer idx : idxs){
                res[i] = trainingData[idx];  // TODO: refactor LV and LD communication
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

        Map<Double, Double> classVotes = new HashMap<>();
        for (int i = 0; i < neighbors.length; i++) {
            LabeledVector neighbor = neighbors[i];
            double classLabel = (double) neighbor.label(); // TODO: handle casting correctly and for different types

            double distance = distanceMeasure.compute(v, neighbor.features()); // TODO: repeated calculation
            if(classVotes.containsKey(classLabel)){
                double classVote = classVotes.get(classLabel);
                classVote += getClassVoteForVector(strategy, distance);
                classVotes.put(classLabel, classVote);
            } else {
                final double value = getClassVoteForVector(strategy, distance);
                classVotes.put(classLabel, value);
            }
        }

        return getClassWithMaxVotes(classVotes);
    }

    private double getClassWithMaxVotes(Map<Double, Double> classVotes) {
        return Collections.max(classVotes.entrySet(), Map.Entry.comparingByValue()).getKey();
    }


    // TODO: handle different strategies
    private double getClassVoteForVector(KNNStrategy strategy, double distance) {

        if (strategy.equals(strategy.WEIGHTED))
            return 1/distance;
        else  return 1.0; // strategy.SIMPLE

    }
}
