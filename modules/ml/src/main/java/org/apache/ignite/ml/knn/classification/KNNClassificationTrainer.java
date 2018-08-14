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

import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.knn.KNNUtils;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/**
 * kNN algorithm trainer to solve multi-class classification task.
 */
public class KNNClassificationTrainer extends SingleLabelDatasetTrainer<KNNClassificationModel> {
    /**
     * Trains model based on the specified data.
     *
     * @param datasetBuilder Dataset builder.
     * @param featureExtractor Feature extractor.
     * @param lbExtractor Label extractor.
     * @return Model.
     */
    @Override public <K, V> KNNClassificationModel fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor) {
        return new KNNClassificationModel(KNNUtils.buildDataset(datasetBuilder, featureExtractor, lbExtractor));
    }
}
