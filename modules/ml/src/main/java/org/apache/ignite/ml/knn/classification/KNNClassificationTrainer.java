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

import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.ml.DatasetTrainer;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.knn.partitions.KNNPartitionContext;
import org.apache.ignite.ml.knn.partitions.KNNPartitionDataBuilderOnHeap;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.svm.SVMLinearBinaryClassificationModel;
import org.apache.ignite.ml.svm.SVMPartitionContext;
import org.apache.ignite.ml.svm.SVMPartitionDataBuilderOnHeap;
import org.jetbrains.annotations.NotNull;

/**
 * kNN algorithm trainer to solve multi-class classification task.
 */
public class KNNClassificationTrainer<K, V> implements DatasetTrainer<K, V, KNNClassificationModel> {
    /**
     * Trains model based on the specified data.
     *
     * @param datasetBuilder   Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor      Label extractor.
     * @param cols             Number of columns.
     * @return Model.
     */
    @Override public KNNClassificationModel fit(DatasetBuilder<K, V> datasetBuilder, IgniteBiFunction<K, V, double[]> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor, int cols) {
        assert datasetBuilder != null;

        PartitionDataBuilder<K, V, KNNPartitionContext, LabeledDataset<Double, LabeledVector>> partDataBuilder = new KNNPartitionDataBuilderOnHeap<>(
            featureExtractor,
            lbExtractor,
            cols
        );

        Dataset<KNNPartitionContext, LabeledDataset<Double, LabeledVector>> dataset = null;

        if (datasetBuilder != null) {
            dataset = datasetBuilder.build(
                (upstream, upstreamSize) -> new KNNPartitionContext(),
                partDataBuilder
            );
        }
        return new KNNClassificationModel<>(dataset);
    }
}


