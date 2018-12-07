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

package org.apache.ignite.ml.tree.randomforest;

import java.util.List;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.MeanValuePredictionsAggregator;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.tree.randomforest.data.TreeRoot;
import org.apache.ignite.ml.tree.randomforest.data.impurity.ImpurityHistogramsComputer;
import org.apache.ignite.ml.tree.randomforest.data.impurity.MSEHistogram;
import org.apache.ignite.ml.tree.randomforest.data.impurity.MSEHistogramComputer;
import org.apache.ignite.ml.tree.randomforest.data.statistics.LeafValuesComputer;
import org.apache.ignite.ml.tree.randomforest.data.statistics.MeanValueStatistic;
import org.apache.ignite.ml.tree.randomforest.data.statistics.RegressionLeafValuesComputer;

/**
 * Regression trainer based on RandomForest algorithm.
 */
public class RandomForestRegressionTrainer
    extends RandomForestTrainer<MeanValueStatistic, MSEHistogram, RandomForestRegressionTrainer> {
    /**
     * Constructs an instance of RandomForestRegressionTrainer.
     *
     * @param meta Features meta.
     */
    public RandomForestRegressionTrainer(List<FeatureMeta> meta) {
        super(meta);
    }

    /** {@inheritDoc} */
    @Override protected RandomForestRegressionTrainer instance() {
        return this;
    }

    /** {@inheritDoc} */
    @Override protected ModelsComposition buildComposition(List<TreeRoot> models) {
        return new ModelsComposition(models, new MeanValuePredictionsAggregator());
    }

    /** {@inheritDoc} */
    @Override protected ImpurityHistogramsComputer<MSEHistogram> createImpurityHistogramsComputer() {
        return new MSEHistogramComputer();
    }

    /** {@inheritDoc} */
    @Override protected LeafValuesComputer<MeanValueStatistic> createLeafStatisticsAggregator() {
        return new RegressionLeafValuesComputer();
    }
}
