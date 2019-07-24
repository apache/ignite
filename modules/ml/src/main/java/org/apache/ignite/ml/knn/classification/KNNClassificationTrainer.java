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

import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.knn.KNNTrainer;
import org.apache.ignite.ml.knn.utils.indices.SpatialIndex;

/**
 * KNN classification model trader that trains model on top of distribtued spatial indices. Be aware that this model is
 * linked with cluster environment it's been built on and can't be saved or used in other places. Under the hood it
 * keeps {@link Dataset} that consists of a set of resources allocated across the cluster.
 */
public class KNNClassificationTrainer extends KNNTrainer<KNNClassificationModel, KNNClassificationTrainer> {
    /** {@inheritDoc} */
    @Override protected KNNClassificationModel convertDatasetIntoModel(
        Dataset<EmptyContext, SpatialIndex<Double>> dataset) {
        return new KNNClassificationModel(dataset, distanceMeasure, k, weighted);
    }

    /** {@inheritDoc} */
    @Override protected KNNClassificationTrainer self() {
        return this;
    }
}
