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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.functions.IgniteBinaryOperator;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.jetbrains.annotations.NotNull;

/**
 * kNN algorithm is a classification algorithm.
 */
public class NewKNNModel<K, V> implements Model<Vector, Double>, Exportable<KNNModelFormat> {

    private final Dataset<KNNPartitionContext, KNNPartitionDataOnHeap> dataset;

    /** Amount of nearest neighbors. */
    protected int k = 5;

    /** Distance measure. */
    protected DistanceMeasure distanceMeasure = new EuclideanDistance();

    /** kNN strategy. */
    protected KNNStrategy stgy = KNNStrategy.SIMPLE;

    protected DatasetBuilder<K, V> datasetBuilder;

    /** Extractor of X matrix row. */
    protected IgniteBiFunction<K, V, double[]> featureExtractor;

    /** Extractor of Y vector value. */
    protected IgniteBiFunction<K, V, Double> lbExtractor;
    private int cols;


    public NewKNNModel(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor, int cols){
        this.datasetBuilder = datasetBuilder;
        this.featureExtractor = featureExtractor;
        this.lbExtractor = lbExtractor;
        this.cols = cols;

        PartitionDataBuilder<K, V, KNNPartitionContext, KNNPartitionDataOnHeap> partDataBuilder = new KNNPartitionDataBuilderOnHeap<>(
            featureExtractor,
            lbExtractor,
            cols
        );

        this.dataset = datasetBuilder.build(
            (upstream, upstreamSize) -> new KNNPartitionContext(),
            partDataBuilder
        );
    }

    /** {@inheritDoc} */
    @Override public Double apply(Vector v) {
        List<LabeledVector> neighbors = findKNearestNeighbors(v);

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
    protected List<LabeledVector> findKNearestNeighbors(Vector v) {
        List<LabeledVector> sosedi = dataset.compute(data -> {
            TreeMap<Double, Set<Integer>> distanceIdxPairs = getDistances(v, data);
            return Arrays.asList(getKClosestVectors(data, distanceIdxPairs));
        }, (a, b) -> a == null ? b : Stream.concat(a.stream(), b.stream()).collect(Collectors.toList()));

        return sosedi;
    }

    /**
     * Iterates along entries in distance map and fill the resulting k-element array.
     *
     * @param trainingData The training data.
     * @param distanceIdxPairs The distance map.
     * @param isCashedDistances Cache distances if true.
     * @return K-nearest neighbors.
     */
    @NotNull private LabeledVector[] getKClosestVectors(KNNPartitionDataOnHeap trainingData,
        TreeMap<Double, Set<Integer>> distanceIdxPairs) {
        LabeledVector[] res = new LabeledVector[k];
        int i = 0;
        final Iterator<Double> iter = distanceIdxPairs.keySet().iterator();
        while (i < k) {
            double key = iter.next();
            Set<Integer> idxs = distanceIdxPairs.get(key);
            for (Integer idx : idxs) {
                res[i] = new LabeledVector(new DenseLocalOnHeapVector(trainingData.getX()[idx]), trainingData.getY()[idx]);
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
    @NotNull private TreeMap<Double, Set<Integer>> getDistances(Vector v, KNNPartitionDataOnHeap trainingData) {
        TreeMap<Double, Set<Integer>> distanceIdxPairs = new TreeMap<>();

        final double[][] x = trainingData.getX();
        for (int i = 0; i < x.length; i++) {
            if (x[i] != null) {
                double distance = distanceMeasure.compute(v, x[i]);
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

    /** TODO: add additional sorting and choosing K most popular */
    private double classify(List<LabeledVector> neighbors, Vector v, KNNStrategy stgy) {
        Map<Double, Double> clsVotes = new HashMap<>();

        for (int i = 0; i < neighbors.size(); i++) {
            LabeledVector neighbor = neighbors.get(i);
            double clsLb = (double)neighbor.label();

            double distance = distanceMeasure.compute(v, neighbor.features());

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
        //res = res * 37 + Arrays.hashCode(training.data());

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        NewKNNModel that = (NewKNNModel)obj;

        return k == that.k && distanceMeasure.equals(that.distanceMeasure) && stgy.equals(that.stgy);
           // && Arrays.deepEquals(training.data(), that.training.data());
    }

    public NewKNNModel<K,V> withK(int k) {
        this.k = k;
        return this;
    }

    public NewKNNModel<K,V> withStrategy(KNNStrategy stgy) {
        this.stgy = stgy;
        return this;
    }

    public NewKNNModel<K,V> withDistanceMeasure(DistanceMeasure distanceMeasure) {
        this.distanceMeasure = distanceMeasure;
        return this;
    }
}
