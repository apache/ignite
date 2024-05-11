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

package org.apache.ignite.ml.knn.utils;

import java.io.Serializable;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.PartitionDataBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.LabeledVectorSet;
import org.apache.ignite.ml.structures.partition.LabeledDatasetPartitionDataBuilderOnHeap;
import org.jetbrains.annotations.Nullable;

/**
 * Helper class for KNNRegression.
 */
public class KNNUtils {
    /**
     * Builds dataset.
     *
     * @param envBuilder Learning environment builder.
     * @param datasetBuilder Dataset builder.
     * @param vectorizer Upstream vectorizer.
     * @return Dataset.
     */
    @Nullable public static <K, V, C extends Serializable> Dataset<EmptyContext, LabeledVectorSet<LabeledVector>> buildDataset(
        LearningEnvironmentBuilder envBuilder,
        DatasetBuilder<K, V> datasetBuilder, Preprocessor<K, V> vectorizer) {
        LearningEnvironment environment = envBuilder.buildForTrainer();
        environment.initDeployingContext(vectorizer);

        PartitionDataBuilder<K, V, EmptyContext, LabeledVectorSet<LabeledVector>> partDataBuilder
            = new LabeledDatasetPartitionDataBuilderOnHeap<>(vectorizer);

        Dataset<EmptyContext, LabeledVectorSet<LabeledVector>> dataset = null;

        if (datasetBuilder != null) {
            dataset = datasetBuilder.build(
                envBuilder,
                (env, upstream, upstreamSize) -> new EmptyContext(),
                partDataBuilder,
                environment
            );
        }
        return dataset;
    }
}
