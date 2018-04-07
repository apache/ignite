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

package org.apache.ignite.ml.partitionedClustering.kmeans;

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.distances.DistanceMeasure;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.partition.LabeledDatasetPartitionDataBuilderOnHeap;
import org.apache.ignite.ml.svm.SVMLinearBinaryClassificationModel;
import org.apache.ignite.ml.svm.SVMPartitionContext;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;
import org.jetbrains.annotations.NotNull;

/**
 */
public class KMeansTrainer implements SingleLabelDatasetTrainer<KMeansModel2> {
    /** Amount of clusters. */
    private int k = 2;

    private int maxIterations = 10;

    private double epsilon = 1e-4;

    /** Distance measure. */
    private DistanceMeasure distance = new EuclideanDistance();


    /**
     * Trains model based on the specified data.
     *
     * @param datasetBuilder   Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor      Label extractor.
     * @return Model.
     */
    @Override public <K, V> KMeansModel2 fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {

        assert datasetBuilder != null;

        PartitionDataBuilder<K, V, SVMPartitionContext, LabeledDataset<Double, LabeledVector>> partDataBuilder = new LabeledDatasetPartitionDataBuilderOnHeap<>(
            featureExtractor,
            lbExtractor
        );


        Vector[] centers;

        try(Dataset<SVMPartitionContext, LabeledDataset<Double, LabeledVector>> dataset = datasetBuilder.build(
            (upstream, upstreamSize) -> new SVMPartitionContext(),
            partDataBuilder
        )) {
            final int cols = dataset.compute(data -> data.colSize(), (a, b) -> a == null ? b : a);
            centers = initClusterCenters(dataset, k);

            boolean converged = false;
            int iteration = 0;

            while (iteration < maxIterations && !converged) {

                iteration++;
            }


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new KMeansModel2(centers, distance);
    }

    private Vector[] initClusterCenters(Dataset<SVMPartitionContext, LabeledDataset<Double, LabeledVector>> dataset, int k) {

        Vector[] initCenters = new DenseLocalOnHeapVector[k];
        for (int i = 0; i < k; i++) {
            
        }

        return new Vector[0];
    }

}


