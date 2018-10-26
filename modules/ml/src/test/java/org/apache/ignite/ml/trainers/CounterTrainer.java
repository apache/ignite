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

package org.apache.ignite.ml.trainers;

import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.BaggingModelTrainer;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.PredictionsAggregator;
import org.apache.ignite.ml.dataset.impl.bootstrapping.BootstrappedDatasetPartition;
import org.apache.ignite.ml.math.functions.IgniteFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;

import java.util.List;
import java.util.Set;

public class CounterTrainer extends BaggingModelTrainer<Model<Vector, Double>, Integer, Void> {
    public CounterTrainer(PredictionsAggregator predictionsAggregator, int featureVectorSize, int maximumFeaturesCntPerMdl, int ensembleSize, double samplePartSizePerMdl) {
        super(predictionsAggregator, featureVectorSize, maximumFeaturesCntPerMdl, ensembleSize, samplePartSizePerMdl);
    }

    @Override
    protected boolean checkState(ModelsComposition mdl) {
        return true;
    }

    @Override
    protected Model<Vector, Double> init() {
        return new ConstModel(0);
    }

    @Override
    protected Integer trainingIteration(
        int partitionIdx,
        BootstrappedDatasetPartition part,
        int modelIdx,
        IgniteFunction<Vector, Vector> projector,
        Void meta) {
        int res = 0;
        for (int i = 0; i < part.getRowsCount(); i++) {
            res += part.getRow(i).counters()[modelIdx];
        }
        return res;
    }

    @Override
    protected Integer reduceTrainingResults(Integer res1, Integer res2) {
        return res1 + res2;
    }

    @Override
    protected Integer identity() {
        return 0;
    }

    @Override
    protected Model<Vector, Double> applyTrainingResultsToModel(Model<Vector, Double> model, Integer res) {
        return new ConstModel(res);
    }

    @Override
    protected Void getMeta(List<Model<Vector, Double>> models) {
        return null;
    }

    @Override
    protected boolean shouldStop(int iterationsCnt, List<Model<Vector, Double>> models, Void meta) {
        return iterationsCnt >= 1;
    }
}
