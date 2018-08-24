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
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.MeanValuePredictionsAggregator;
import org.apache.ignite.ml.dataset.feature.BucketMeta;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.dataset.impl.bagging.BootstrappedVector;
import org.apache.ignite.ml.tree.randomforest.data.impurity.MSEHistogram;

public class RandomForestRegressionTrainer extends RandomForestTrainer<IgniteBiTuple<Double, Integer>, MSEHistogram, RandomForestRegressionTrainer> {
    public RandomForestRegressionTrainer(List<FeatureMeta> meta) {
        super(meta);
    }

    @Override protected RandomForestRegressionTrainer instance() {
        return this;
    }

    @Override protected ModelsComposition buildComposition(List<TreeRoot> models) {
        return new ModelsComposition(models, new MeanValuePredictionsAggregator());
    }

    @Override protected MSEHistogram createImpurityComputer(int sampleId, BucketMeta meta) {
        return new MSEHistogram(sampleId, meta);
    }

    @Override protected void addElementToLeafStat(IgniteBiTuple<Double, Integer> leafStatAggr, BootstrappedVector vec, int sampleId) {
        leafStatAggr.set1(leafStatAggr.get1() + vec.getLabel() * vec.getRepetitionsCounters()[sampleId]);
        leafStatAggr.set2(leafStatAggr.get2() + vec.getRepetitionsCounters()[sampleId]);
    }

    @Override protected IgniteBiTuple<Double, Integer> mergeLeafStats(IgniteBiTuple<Double, Integer> leafStatAggr1,
        IgniteBiTuple<Double, Integer> leafStatAggr2) {

        leafStatAggr1.set1(leafStatAggr1.get1() + leafStatAggr2.get1());
        leafStatAggr1.set2(leafStatAggr1.get2() + leafStatAggr2.get2());
        return leafStatAggr1;
    }

    @Override protected IgniteBiTuple<Double, Integer> createLeafStatsAggregator(int sampleId) {
        return new IgniteBiTuple<>(0.0, 0);
    }

    @Override protected double computeLeafValue(IgniteBiTuple<Double, Integer> stat) {
        return stat.get1() / stat.get2();
    }
}
