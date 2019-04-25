/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.ml.knn.classification;

import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.knn.KNNUtils;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/**
 * kNN algorithm trainer to solve multi-class classification task.
 */
public class KNNClassificationTrainer extends SingleLabelDatasetTrainer<KNNClassificationModel> {
    /** {@inheritDoc} */
    @Override public <K, V> KNNClassificationModel fit(DatasetBuilder<K, V> datasetBuilder,
                                                       Preprocessor<K, V> extractor) {

        return updateModel(null, datasetBuilder, extractor);
    }

    /** {@inheritDoc} */
    @Override protected <K, V> KNNClassificationModel updateModel(KNNClassificationModel mdl,
                                                                  DatasetBuilder<K, V> datasetBuilder,
                                                                  Preprocessor<K, V> extractor) {

        KNNClassificationModel res = new KNNClassificationModel(KNNUtils.buildDataset(envBuilder, datasetBuilder, extractor));
        if (mdl != null)
            res.copyStateFrom(mdl);
        return res;
    }

    /** {@inheritDoc} */
    @Override public KNNClassificationTrainer withEnvironmentBuilder(LearningEnvironmentBuilder envBuilder) {
        return (KNNClassificationTrainer)super.withEnvironmentBuilder(envBuilder);
    }

    /** {@inheritDoc} */
    @Override public boolean isUpdateable(KNNClassificationModel mdl) {
        return true;
    }
}
