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

import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.knn.KNNTrainer;
import org.apache.ignite.ml.knn.utils.indices.SpatialIndex;

/**
 * KNN regression model trader that trains model on top of distributed spatial indices. Be aware that this model is
 * linked with cluster environment it's been built on and can't be saved or used in other places. Under the hood it
 * keeps {@link Dataset} that consists of a set of resources allocated across the cluster.
 */
public class KNNRegressionTrainer extends KNNTrainer<KNNRegressionModel, KNNRegressionTrainer> {
    /** {@inheritDoc} */
    @Override protected KNNRegressionModel convertDatasetIntoModel(
        Dataset<EmptyContext, SpatialIndex<Double>> dataset) {
        return new KNNRegressionModel(dataset, distanceMeasure, k, weighted);
    }

    /** {@inheritDoc} */
    @Override protected KNNRegressionTrainer self() {
        return this;
    }
}
