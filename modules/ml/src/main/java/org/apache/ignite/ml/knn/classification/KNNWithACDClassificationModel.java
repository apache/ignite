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

package org.apache.ignite.ml.knn.classification;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.util.ModelTrace;
import org.jetbrains.annotations.NotNull;

/**
 * kNN algorithm model to solve multi-class classification task.
 */
public class KNNWithACDClassificationModel implements Model<Vector, Double>, Exportable<KNNModelFormat> {
    /** */
    private static final long serialVersionUID = -127386523291350345L;

    private final LabeledDataset<ProbableLabel, LabeledVector> candidates;

    /** Amount of nearest neighbors. */
    protected int k = 5;

    /** Distance measure. */
    protected DistanceMeasure distanceMeasure = new EuclideanDistance();

    /** kNN strategy. */
    protected KNNStrategy stgy = KNNStrategy.SIMPLE;


    public KNNWithACDClassificationModel(LabeledDataset<ProbableLabel, LabeledVector> centers) {
       this.candidates = centers;
    }

    /** {@inheritDoc} */
    @Override public Double apply(Vector v) {

            List<LabeledVector> neighbors = findKNearestNeighbors(v);

            return classify(neighbors, v, stgy);

    }

    /** */
    @Override public <P> void saveModel(Exporter<KNNModelFormat, P> exporter, P path) {
        KNNModelFormat mdlData = new KNNModelFormat(k, distanceMeasure, stgy);
        exporter.save(mdlData, path);
    }

    /**
     * Set up parameter of the kNN model.
     * @param k Amount of nearest neighbors.
     * @return Model.
     */
    public KNNWithACDClassificationModel withK(int k) {
        this.k = k;
        return this;
    }

    /**
     * Set up parameter of the kNN model.
     * @param stgy Strategy of calculations.
     * @return Model.
     */
    public KNNWithACDClassificationModel withStrategy(KNNStrategy stgy) {
        this.stgy = stgy;
        return this;
    }

    /**
     * Set up parameter of the kNN model.
     * @param distanceMeasure Distance measure.
     * @return Model.
     */
    public KNNWithACDClassificationModel withDistanceMeasure(DistanceMeasure distanceMeasure) {
        this.distanceMeasure = distanceMeasure;
        return this;
    }

    /**
     * The main idea is calculation all distance pairs between given vector and all vectors in training set, sorting
     * them and finding k vectors with min distance with the given vector.
     *
     * @param v The given vector.
     * @return K-nearest neighbors.
     */
    protected List<LabeledVector> findKNearestNeighbors(Vector v) {


        return Arrays.asList(getKClosestVectors(getDistances(v)));
    }



    /**
     * Iterates along entries in distance map and fill the resulting k-element array.
     * @param distanceIdxPairs The distance map.
     * @return K-nearest neighbors.
     */
    @NotNull private LabeledVector[] getKClosestVectors(
        TreeMap<Double, Set<Integer>> distanceIdxPairs) {
        LabeledVector[] res;

        if (candidates.rowSize() <= k) {
            res = new LabeledVector[candidates.rowSize()];
            for (int i = 0; i < candidates.rowSize(); i++)
                res[i] = candidates.getRow(i);
        }
        else {
            res = new LabeledVector[k];
            int i = 0;
            final Iterator<Double> iter = distanceIdxPairs.keySet().iterator();
            while (i < k) {
                double key = iter.next();
                Set<Integer> idxs = distanceIdxPairs.get(key);
                for (Integer idx : idxs) {
                    res[i] = candidates.getRow(idx);
                    i++;
                    if (i >= k)
                        break; // go to next while-loop iteration
                }
            }
        }

        return res;
    }

    /**
     * Computes distances between given vector and each vector in training dataset.
     *
     * @param v The given vector.
     * @return Key - distanceMeasure from given features before features with idx stored in value. Value is presented
     * with Set because there can be a few vectors with the same distance.
     */
    @NotNull private TreeMap<Double, Set<Integer>> getDistances(Vector v) {
        TreeMap<Double, Set<Integer>> distanceIdxPairs = new TreeMap<>();

        for (int i = 0; i < candidates.rowSize(); i++) {

            LabeledVector labeledVector = candidates.getRow(i);
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
    private double classify(List<LabeledVector> neighbors, Vector v, KNNStrategy stgy) {
        Map<Double, Double> clsVotes = new HashMap<>();

        for (LabeledVector neighbor : neighbors) {
            TreeMap<Double, Double> probableClsLb = ((ProbableLabel)neighbor.label()).clsLbls;

            double distance = distanceMeasure.compute(v, neighbor.features());

            // let's think
            probableClsLb.forEach((label, probability) -> {

                double cnt = clsVotes.containsKey(label) ? clsVotes.get(label) : 0;
                centroidStat.put(lb, cnt + 1);


            });

            for (int clsIdx = 0; clsIdx < probableClsLb.length; clsIdx++) {

                final double clsLbl = clsIdx + 1;
                if (clsVotes.containsKey(clsLbl)) {
                    double clsVote = clsVotes.get(clsLbl);
                    clsVote += getClassVoteForVector(stgy, distance) * probableClsLb[clsIdx];
                    clsVotes.put(clsLbl, clsVote);
                }
                else {
                    final double val = getClassVoteForVector(stgy, distance);
                    clsVotes.put(clsLbl, val);
                }

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

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        KNNWithACDClassificationModel that = (KNNWithACDClassificationModel)obj;

        return k == that.k && distanceMeasure.equals(that.distanceMeasure) && stgy.equals(that.stgy);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return toString(false);
    }

    /** {@inheritDoc} */
    @Override public String toString(boolean pretty) {
        return ModelTrace.builder("KNNClassificationModel", pretty)
            .addField("k", String.valueOf(k))
            .addField("measure", distanceMeasure.getClass().getSimpleName())
            .addField("strategy", stgy.name())
            .toString();
    }
}
