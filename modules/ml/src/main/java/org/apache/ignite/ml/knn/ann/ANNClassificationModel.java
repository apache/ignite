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

package org.apache.ignite.ml.knn.ann;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.knn.NNClassificationModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.LabeledVectorSet;
import org.apache.ignite.ml.util.ModelTrace;
import org.jetbrains.annotations.NotNull;

/**
 * ANN model to predict labels in multi-class classification task.
 */
public final class ANNClassificationModel extends NNClassificationModel {
    /** */
    private static final long serialVersionUID = -127312378991350345L;

    /** The labeled set of candidates. */
    private final LabeledVectorSet<LabeledVector> candidates;

    /** Centroid statistics. */
    private final ANNClassificationTrainer.CentroidStat centroindsStat;

    /**
     * Build the model based on a candidates set.
     * @param centers The candidates set.
     * @param centroindsStat The stat about centroids.
     */
    public ANNClassificationModel(LabeledVectorSet<LabeledVector> centers,
        ANNClassificationTrainer.CentroidStat centroindsStat) {
       this.candidates = centers;
       this.centroindsStat = centroindsStat;
    }

    /** */
    public LabeledVectorSet<LabeledVector> getCandidates() {
        return candidates;
    }

    /** */
    public ANNClassificationTrainer.CentroidStat getCentroindsStat() {
        return centroindsStat;
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector v) {
            List<LabeledVector> neighbors = findKNearestNeighbors(v);
            return classify(neighbors, v, weighted);
    }

    /** */
    @Override public <P> void saveModel(Exporter<KNNModelFormat, P> exporter, P path) {
        ANNModelFormat mdlData = new ANNModelFormat(k, distanceMeasure, weighted, candidates, centroindsStat);
        exporter.save(mdlData, path);
    }

    /**
     * The main idea is calculation all distance pairs between given vector and all centroids in candidates set, sorting
     * them and finding k vectors with min distance with the given vector.
     *
     * @param v The given vector.
     * @return K-nearest neighbors.
     */
    private List<LabeledVector> findKNearestNeighbors(Vector v) {
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
    private double classify(List<LabeledVector> neighbors, Vector v, boolean weighted) {
        Map<Double, Double> clsVotes = new HashMap<>();

        for (LabeledVector neighbor : neighbors) {
            TreeMap<Double, Double> probableClsLb = ((ProbableLabel)neighbor.label()).clsLbls;

            double distance = distanceMeasure.compute(v, neighbor.features());

            // we predict class label, not the probability vector (it need here another math with counting of votes)
            probableClsLb.forEach((label, probability) -> {
                double cnt = clsVotes.containsKey(label) ? clsVotes.get(label) : 0;
                clsVotes.put(label, cnt + probability * getClassVoteForVector(weighted, distance));
            });
        }
        return getClassWithMaxVotes(clsVotes);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + k;
        res = res * 37 + distanceMeasure.hashCode();
        res = res * 37 + Boolean.hashCode(weighted);
        res = res * 37 + candidates.hashCode();

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        ANNClassificationModel that = (ANNClassificationModel)obj;

        return k == that.k
            && distanceMeasure.equals(that.distanceMeasure)
            && weighted == that.weighted
            && candidates.equals(that.candidates);
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
            .addField("weighted", String.valueOf(weighted))
            .addField("amount of candidates", String.valueOf(candidates.rowSize()))
            .toString();
    }
}
