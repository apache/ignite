/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.knn.classification;

import org.apache.ignite.ml.composition.CompositionUtils;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.knn.KNNUtils;
import org.apache.ignite.ml.trainers.FeatureLabelExtractor;
import org.apache.ignite.ml.trainers.SingleLabelDatasetTrainer;

/**
 * kNN algorithm trainer to solve multi-class classification task.
 */
public class KNNClassificationTrainer extends SingleLabelDatasetTrainer<KNNClassificationModel> {
    /** {@inheritDoc} */
    @Override public <K, V> KNNClassificationModel fit(DatasetBuilder<K, V> datasetBuilder,
        FeatureLabelExtractor<K, V, Double> extractor) {

        return updateModel(null, datasetBuilder, extractor);
    }

    /** {@inheritDoc} */
    @Override protected <K, V> KNNClassificationModel updateModel(KNNClassificationModel mdl,
        DatasetBuilder<K, V> datasetBuilder,
        FeatureLabelExtractor<K, V, Double> extractor) {

        KNNClassificationModel res = new KNNClassificationModel(KNNUtils.buildDataset(envBuilder,
            datasetBuilder,
            CompositionUtils.asFeatureExtractor(extractor),
            CompositionUtils.asLabelExtractor(extractor)));
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
