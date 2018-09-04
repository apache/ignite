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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.knn.NNClassificationModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.LabeledVectorSet;

/**
 * kNN algorithm model to solve multi-class classification task.
 */
public class KNNClassificationModel extends NNClassificationModel implements Exportable<KNNModelFormat> {
    /** */
    private static final long serialVersionUID = -127386523291350345L;

    /** Datasets. */
    private List<Dataset<EmptyContext, LabeledVectorSet<Double, LabeledVector>>> datasets;

    /**
     * Builds the model via prepared dataset.
     *
     * @param dataset Specially prepared object to run algorithm over it.
     */
    public KNNClassificationModel(Dataset<EmptyContext, LabeledVectorSet<Double, LabeledVector>> dataset) {
        this.datasets = new ArrayList<>();
        if (dataset != null)
            datasets.add(dataset);
    }

    /** {@inheritDoc} */
    @Override public Double apply(Vector v) {
        if (!datasets.isEmpty()) {
            List<LabeledVector> neighbors = findKNearestNeighbors(v);

            return classify(neighbors, v, stgy);
        } else {
            throw new IllegalStateException("The train kNN dataset is null");
        }
    }

    /** */
    @Override public <P> void saveModel(Exporter<KNNModelFormat, P> exporter, P path) {
        KNNModelFormat mdlData = new KNNModelFormat(k, distanceMeasure, stgy);
        exporter.save(mdlData, path);
    }

    /**
     * The main idea is calculation all distance pairs between given vector and all vectors in training set, sorting
     * them and finding k vectors with min distance with the given vector.
     *
     * @param v The given vector.
     * @return K-nearest neighbors.
     */
    protected List<LabeledVector> findKNearestNeighbors(Vector v) {
        List<LabeledVector> neighborsFromPartitions = datasets.stream()
            .flatMap(dataset -> findKNearestNeighborsInDataset(v, dataset).stream())
            .collect(Collectors.toList());

        LabeledVectorSet<Double, LabeledVector> neighborsToFilter = buildLabeledDatasetOnListOfVectors(neighborsFromPartitions);

        return Arrays.asList(getKClosestVectors(neighborsToFilter, getDistances(v, neighborsToFilter)));
    }

    private List<LabeledVector> findKNearestNeighborsInDataset(Vector v,
        Dataset<EmptyContext, LabeledVectorSet<Double, LabeledVector>> dataset) {
        List<LabeledVector> neighborsFromPartitions = dataset.compute(data -> {
            TreeMap<Double, Set<Integer>> distanceIdxPairs = getDistances(v, data);
            return Arrays.asList(getKClosestVectors(data, distanceIdxPairs));
        }, (a, b) -> {
            if (a == null)
                return b == null ? new ArrayList<>() : b;
            if (b == null)
                return a;
            return Stream.concat(a.stream(), b.stream()).collect(Collectors.toList());
        });

        if(neighborsFromPartitions == null)
            return Collections.emptyList();

        LabeledVectorSet<Double, LabeledVector> neighborsToFilter = buildLabeledDatasetOnListOfVectors(neighborsFromPartitions);

        return Arrays.asList(getKClosestVectors(neighborsToFilter, getDistances(v, neighborsToFilter)));
    }

    /** */
    private double classify(List<LabeledVector> neighbors, Vector v, NNStrategy stgy) {
        Map<Double, Double> clsVotes = new HashMap<>();

        for (LabeledVector neighbor : neighbors) {
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

    /**
     * Copy parameters from other model and save all datasets from it.
     *
     * @param model Model.
     */
    public void copyStateFrom(KNNClassificationModel model) {
        this.copyParametersFrom(model);
        datasets.addAll(model.datasets);
    }
}
