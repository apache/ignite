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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.environment.deploy.DeployableObject;
import org.apache.ignite.ml.inference.json.JSONModel;
import org.apache.ignite.ml.inference.json.JSONWritable;
import org.apache.ignite.ml.knn.NNClassificationModel;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.LabeledVectorSet;
import org.apache.ignite.ml.util.ModelTrace;

/**
 * ANN model to predict labels in multi-class classification task.
 */
public final class ANNClassificationModel extends NNClassificationModel implements JSONWritable, DeployableObject {
    /** */
    private static final long serialVersionUID = -127312378991350345L;

    /** The labeled set of candidates. */
    private LabeledVectorSet<LabeledVector> candidates;

    /** Centroid statistics. */
    private ANNClassificationTrainer.CentroidStat centroindsStat;

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
    private ANNClassificationModel() {
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
    private LabeledVector[] getKClosestVectors(
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
    private TreeMap<Double, Set<Integer>> getDistances(Vector v) {
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

    /** {@inheritDoc} */
    @JsonIgnore
    @Override public List<Object> getDependencies() {
        return Collections.emptyList();
    }

    /** Loads ANNClassificationModel from JSON file. */
    public static ANNClassificationModel fromJSON(Path path) {
        ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        ANNJSONExportModel exportModel;
        try {
            exportModel = mapper
                .readValue(new File(path.toAbsolutePath().toString()), ANNJSONExportModel.class);

            return exportModel.convert();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /** {@inheritDoc} */
    @Override public void toJSON(Path path) {
        ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

        try {
            ANNJSONExportModel exportModel = new ANNJSONExportModel(
                System.currentTimeMillis(),
                "ann_" + UUID.randomUUID(),
                ANNClassificationModel.class.getSimpleName()
            );
            List<double[]> listOfCandidates = new ArrayList<>();
            ProbableLabel[] labels = new ProbableLabel[candidates.rowSize()];
            for (int i = 0; i < candidates.rowSize(); i++) {
                labels[i] = (ProbableLabel) candidates.getRow(i).getLb();
                listOfCandidates.add(candidates.features(i).asArray());
            }

            exportModel.candidateFeatures = listOfCandidates;
            exportModel.distanceMeasure = distanceMeasure;
            exportModel.k = k;
            exportModel.weighted = weighted;
            exportModel.candidateLabels = labels;
            exportModel.centroindsStat = centroindsStat;

            File file = new File(path.toAbsolutePath().toString());
            mapper.writeValue(file, exportModel);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /** */
    public static class ANNJSONExportModel extends JSONModel {
        /** Centers of clusters. */
        public List<double[]> candidateFeatures;

        public ProbableLabel[] candidateLabels;

        /** Distance measure. */
        public DistanceMeasure distanceMeasure;

        /** Amount of nearest neighbors. */
        public int k;

        /** kNN strategy. */
        public boolean weighted;

        /** Centroid statistics. */
        public ANNClassificationTrainer.CentroidStat centroindsStat;

        /** */
        public ANNJSONExportModel(Long timestamp, String uid, String modelClass) {
            super(timestamp, uid, modelClass);
        }

        /** */
        @JsonCreator
        public ANNJSONExportModel() {
        }

        /** {@inheritDoc} */
        @Override public ANNClassificationModel convert() {
            if (candidateFeatures == null || candidateFeatures.isEmpty())
                throw new IllegalArgumentException("Loaded list of candidates is empty. It should be not empty.");

            double[] firstRow = candidateFeatures.get(0);
            LabeledVectorSet<LabeledVector> candidatesForANN = new LabeledVectorSet<>(candidateFeatures.size(), firstRow.length);
            LabeledVector<Double>[] data = new LabeledVector[candidateFeatures.size()];
            for (int i = 0; i < candidateFeatures.size(); i++) {
                data[i] = new LabeledVector(VectorUtils.of(candidateFeatures.get(i)), candidateLabels[i]);
            }
            candidatesForANN.setData(data);

            ANNClassificationModel mdl = new ANNClassificationModel(candidatesForANN, centroindsStat);

            mdl.withDistanceMeasure(distanceMeasure);
            mdl.withK(k);
            mdl.withWeighted(weighted);
            return mdl;
        }
    }
}
