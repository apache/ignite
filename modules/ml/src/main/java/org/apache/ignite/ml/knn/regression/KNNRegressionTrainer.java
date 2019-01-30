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

package org.apache.ignite.ml.knn.regression;

import org.apache.ignite.ml.composition.CompositionUtils;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.knn.KNNUtils;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/**
 * kNN algorithm trainer to solve regression task.
 */
public class KNNRegressionTrainer extends SingleLabelDatasetTrainer<KNNRegressionModel> {
    /** {@inheritDoc} */
    @Override public <K, V> KNNRegressionModel fit(DatasetBuilder<K, V> datasetBuilder,
        FeatureLabelExtractor<K, V, Double> extractor) {

        return updateModel(null, datasetBuilder, extractor);
    }

    /** {@inheritDoc} */
    @Override public <K, V> KNNRegressionModel updateModel(
        KNNRegressionModel mdl,
        DatasetBuilder<K, V> datasetBuilder,
        FeatureLabelExtractor<K, V, Double> extractor) {

        KNNRegressionModel res = new KNNRegressionModel(KNNUtils.buildDataset(envBuilder, datasetBuilder,
            CompositionUtils.asFeatureExtractor(extractor),
            CompositionUtils.asLabelExtractor(extractor)));
        if (mdl != null)
            res.copyStateFrom(mdl);
        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean isUpdateable(KNNRegressionModel mdl) {
        return true;
    }
}
