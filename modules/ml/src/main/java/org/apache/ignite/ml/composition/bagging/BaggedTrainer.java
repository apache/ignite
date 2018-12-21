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

package org.apache.ignite.ml.composition.bagging;

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.predictionsaggregator.PredictionsAggregator;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.trainers.DatasetTrainer;

public class BaggedTrainer<I, O, M extends Model<I, O>, L, T extends DatasetTrainer<M, L>> extends
DatasetTrainer<BaggedModel<I, O, M>, L> {
    private final DatasetTrainer<M, L> tr;
    private final PredictionsAggregator aggregator;
    private final int ensembleSize;
    private final double subsampleRatio;
    private final int featureVectorSize;
    private final int featuresSubspaceDim;

    public BaggedTrainer(DatasetTrainer<M, L> tr,
        PredictionsAggregator aggregator, int ensembleSize, double subsampleRatio, int featureVectorSize,
        int featuresSubspaceDim) {
        this.tr = tr;
        this.aggregator = aggregator;
        this.ensembleSize = ensembleSize;
        this.subsampleRatio = subsampleRatio;
        this.featureVectorSize = featureVectorSize;
        this.featuresSubspaceDim = featuresSubspaceDim;
    }

    private DatasetTrainer<M, L> getTrainer() {

    }

    @Override public <K, V> BaggedModel<I, O, M> fit(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        return null;
    }

    @Override protected boolean checkState(BaggedModel<I, O, M> mdl) {
        return false;
    }

    @Override
    protected <K, V> BaggedModel<I, O, M> updateModel(BaggedModel<I, O, M> mdl, DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, L> lbExtractor) {
        return null;
    }
}
