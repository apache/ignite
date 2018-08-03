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

package org.apache.ignite.ml.tree.boosting;

import java.util.Arrays;
import java.util.List;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.boosting.GDBBinaryClassifierTrainer;
import org.apache.ignite.ml.composition.predictionsaggregator.WeightedPredictionsAggregator;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.DatasetBuilder;
import org.apache.ignite.ml.dataset.primitive.builder.context.EmptyContextBuilder;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.tree.DecisionTreeRegressionTrainer;
import org.apache.ignite.ml.tree.data.DecisionTreeData;
import org.apache.ignite.ml.tree.data.DecisionTreeDataBuilder;
import org.apache.ignite.ml.tree.data.TreeDataIndex;
import org.apache.ignite.ml.tree.impurity.ImpurityMeasureCalculator;
import org.apache.ignite.ml.tree.impurity.mse.MSEImpurityMeasure;
import org.apache.ignite.ml.tree.impurity.mse.MSEImpurityMeasureCalculator;
import org.jetbrains.annotations.NotNull;

/**
 * Implementation of Gradient Boosting Classifier Trainer on trees.
 */
public class GDBBinaryClassifierOnTreesTrainer extends GDBBinaryClassifierTrainer<DecisionTreeRegressionTrainer> {
    /** Max depth. */
    private final int maxDepth;

    /** Min impurity decrease. */
    private final double minImpurityDecrease;

    /** Use index structure instead of using sorting while learning. */
    private boolean useIndex = true;

    /**
     * Constructs instance of GDBBinaryClassifierOnTreesTrainer.
     *
     * @param gradStepSize Gradient step size.
     * @param cntOfIterations Count of iterations.
     * @param maxDepth Max depth.
     * @param minImpurityDecrease Min impurity decrease.
     */
    public GDBBinaryClassifierOnTreesTrainer(double gradStepSize, Integer cntOfIterations,
        int maxDepth, double minImpurityDecrease) {

        super(gradStepSize, cntOfIterations);
        this.maxDepth = maxDepth;
        this.minImpurityDecrease = minImpurityDecrease;
    }

    protected <K, V> void learnModels(DatasetBuilder<K, V> datasetBuilder,
        IgniteBiFunction<K, V, Vector> featureExtractor, IgniteBiFunction<K, V, Double> lbExtractor, Double mean,
        Long sampleSize, List<Model<Vector, Double>> models, double[] compositionWeights) {

        try (Dataset<EmptyContext, DecisionTreeData> dataset = datasetBuilder.build(
            new EmptyContextBuilder<>(),
            new DecisionTreeDataBuilder<>(featureExtractor, lbExtractor, useIndex)
        )) {
            for (int i = 0; i < cntOfIterations; i++) {
                double[] weights = Arrays.copyOf(compositionWeights, i);
                WeightedPredictionsAggregator aggregator = new WeightedPredictionsAggregator(weights, mean);
                Model<Vector, Double> currComposition = new ModelsComposition(models, aggregator);

                dataset.compute(part -> {
                    if(part.getCopyOfOriginalLabels() == null)
                        part.setCopyOfOriginalLabels(Arrays.copyOf(part.getLabels(), part.getLabels().length));

                    for(int j = 0; j < part.getLabels().length; j++) {
                        double mdlAnswer = currComposition.apply(VectorUtils.of(part.getFeatures()[j]));
                        double originalLabelValue = externalLabelToInternal(part.getCopyOfOriginalLabels()[j]);
                        double grad = -lossGradient.apply(sampleSize, originalLabelValue, mdlAnswer);
                        part.getLabels()[j] = grad;
                    }
                });

                long startTs = System.currentTimeMillis();
                models.add(buildBaseModelTrainer().fit(dataset));
                double learningTime = (double)(System.currentTimeMillis() - startTs) / 1000.0;
                environment.logger(getClass()).log(MLLogger.VerboseLevel.LOW, "One model training time was %.2fs", learningTime);
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @NotNull @Override protected DecisionTreeRegressionTrainer buildBaseModelTrainer() {
        return new DecisionTreeRegressionTrainer(maxDepth, minImpurityDecrease).withUseIndex(useIndex);
    }

    /**
     * Sets useIndex parameter and returns trainer instance.
     *
     * @param useIndex Use index.
     * @return Decision tree trainer.
     */
    public GDBBinaryClassifierOnTreesTrainer withUseIndex(boolean useIndex) {
        this.useIndex = useIndex;
        return this;
    }
}
