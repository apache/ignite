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

package org.apache.ignite.ml.knn;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.environment.deploy.DeployableObject;
import org.apache.ignite.ml.knn.ann.KNNModelFormat;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.LabeledVectorSet;
import org.apache.ignite.ml.util.ModelTrace;
import org.jetbrains.annotations.NotNull;

/**
 * Common methods and fields for all kNN and aNN models
 * to predict label based on neighbours' labels.
 */
public abstract class NNClassificationModel implements IgniteModel<Vector, Double>, Exportable<KNNModelFormat>,
    DeployableObject {
    /** Amount of nearest neighbors. */
    protected int k = 5;

    /** Distance measure. */
    protected DistanceMeasure distanceMeasure = new EuclideanDistance();

    /** kNN strategy. */
    protected boolean weighted;

    /**
     * Set up parameter of the NN model.
     * @param k Amount of nearest neighbors.
     * @return Model.
     */
    public NNClassificationModel withK(int k) {
        this.k = k;
        return this;
    }

    /**
     * Sets up {@code weighted} parameter.
     *
     * @param weighted Weighted or not.
     * @return This instance.
     */
    public NNClassificationModel withWeighted(boolean weighted) {
        this.weighted = weighted;
        return this;
    }

    /**
     * Set up parameter of the NN model.
     * @param distanceMeasure Distance measure.
     * @return Model.
     */
    public NNClassificationModel withDistanceMeasure(DistanceMeasure distanceMeasure) {
        this.distanceMeasure = distanceMeasure;
        return this;
    }

    /** */
    protected LabeledVectorSet<LabeledVector> buildLabeledDatasetOnListOfVectors(
        List<LabeledVector> neighborsFromPartitions) {
        LabeledVector[] arr = new LabeledVector[neighborsFromPartitions.size()];
        for (int i = 0; i < arr.length; i++)
            arr[i] = neighborsFromPartitions.get(i);

        return new LabeledVectorSet<LabeledVector>(arr);
    }

    /**
     * Iterates along entries in distance map and fill the resulting k-element array.
     *
     * @param trainingData The training data.
     * @param distanceIdxPairs The distance map.
     * @return K-nearest neighbors.
     */
    @NotNull protected LabeledVector[] getKClosestVectors(LabeledVectorSet<LabeledVector> trainingData,
                                                          TreeMap<Double, Set<Integer>> distanceIdxPairs) {
        LabeledVector[] res;

        if (trainingData.rowSize() <= k) {
            res = new LabeledVector[trainingData.rowSize()];
            for (int i = 0; i < trainingData.rowSize(); i++)
                res[i] = trainingData.getRow(i);
        }
        else {
            res = new LabeledVector[k];
            int i = 0;
            final Iterator<Double> iter = distanceIdxPairs.keySet().iterator();
            while (i < k) {
                double key = iter.next();
                Set<Integer> idxs = distanceIdxPairs.get(key);
                for (Integer idx : idxs) {
                    res[i] = trainingData.getRow(idx);
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
     * @param trainingData The training dataset.
     * @return Key - distanceMeasure from given features before features with idx stored in value. Value is presented
     * with Set because there can be a few vectors with the same distance.
     */
    @NotNull protected TreeMap<Double, Set<Integer>> getDistances(Vector v, LabeledVectorSet<LabeledVector> trainingData) {
        TreeMap<Double, Set<Integer>> distanceIdxPairs = new TreeMap<>();

        for (int i = 0; i < trainingData.rowSize(); i++) {

            LabeledVector labeledVector = trainingData.getRow(i);
            if (labeledVector != null) {
                double distance = distanceMeasure.compute(v, labeledVector.features());
                putDistanceIdxPair(distanceIdxPairs, i, distance);
            }
        }
        return distanceIdxPairs;
    }

    /** */
    protected void putDistanceIdxPair(Map<Double, Set<Integer>> distanceIdxPairs, int i, double distance) {
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
    protected double getClassWithMaxVotes(Map<Double, Double> clsVotes) {
        return Collections.max(clsVotes.entrySet(), Map.Entry.comparingByValue()).getKey();
    }

    /**
     *
     */
    protected double getClassVoteForVector(boolean weighted, double distance) {
        return weighted ? 1.0 / distance : 1.0;
    }

    /** */
    public DistanceMeasure getDistanceMeasure() {
        return distanceMeasure;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = 1;

        res = res * 37 + k;
        res = res * 37 + distanceMeasure.hashCode();
        res = res * 37 + Boolean.hashCode(weighted);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        if (this == obj)
            return true;

        if (obj == null || getClass() != obj.getClass())
            return false;

        NNClassificationModel that = (NNClassificationModel)obj;

        return k == that.k && distanceMeasure.equals(that.distanceMeasure) && weighted == that.weighted;
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
            .toString();
    }

    /**
     * Sets parameters from other model to this model.
     *
     * @param mdl Model.
     */
    protected void copyParametersFrom(NNClassificationModel mdl) {
        this.k = mdl.k;
        this.distanceMeasure = mdl.distanceMeasure;
        this.weighted = mdl.weighted;
    }

    /** {@inheritDoc} */
    @Override public abstract <P> void saveModel(Exporter<KNNModelFormat, P> exporter, P path);

    /** {@inheritDoc} */
    @Override public List<Object> getDependencies() {
        return Collections.singletonList(distanceMeasure);
    }
}
