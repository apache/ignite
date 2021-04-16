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

package org.apache.ignite.examples.ml.regression.logistic.bagged;

import java.io.IOException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ml.util.MLSandboxDatasets;
import org.apache.ignite.examples.ml.util.SandboxMLCache;
import org.apache.ignite.ml.composition.bagging.BaggedTrainer;
import org.apache.ignite.ml.composition.predictionsaggregator.OnMajorityPredictionsAggregator;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionSGDTrainer;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.MetricName;
import org.apache.ignite.ml.trainers.TrainerTransformers;

/**
 * This example shows how bagging technique may be applied to arbitrary trainer. As an example (a bit synthetic)
 * logistic regression is considered.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with test data points (based on the
 * <a href="https://en.wikipedia.org/wiki/Iris_flower_data_set"></a>Iris dataset</a>).</p>
 * <p>
 * After that it trains bootstrapped (or bagged) version of logistic regression trainer. Bootstrapping is done on both
 * samples and features (<a href="https://en.wikipedia.org/wiki/Bootstrap_aggregating"></a>Samples bagging</a>,
 * <a href="https://en.wikipedia.org/wiki/Random_subspace_method"></a>Features bagging</a>).</p>
 * <p>
 * Finally, this example applies cross-validation to resulted model and prints accuracy if each fold.</p>
 */
public class BaggedLogisticRegressionSGDTrainerExample {
    /**
     * Run example.
     */
    public static void main(String[] args) throws IOException {
        System.out.println();
        System.out.println(">>> Logistic regression model over partitioned dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = null;
            try {
                dataCache = new SandboxMLCache(ignite).fillCacheWith(MLSandboxDatasets.TWO_CLASSED_IRIS);

                System.out.println(">>> Create new logistic regression trainer object.");
                LogisticRegressionSGDTrainer trainer = new LogisticRegressionSGDTrainer()
                    .withUpdatesStgy(new UpdatesStrategy<>(
                        new SimpleGDUpdateCalculator(0.2),
                        SimpleGDParameterUpdate.SUM_LOCAL,
                        SimpleGDParameterUpdate.AVG
                    ))
                    .withMaxIterations(100)
                    .withLocIterations(10)
                    .withBatchSize(10)
                    .withSeed(123L);

                System.out.println(">>> Perform the training to get the model.");

                BaggedTrainer<Double> baggedTrainer = TrainerTransformers.makeBagged(
                    trainer,
                    10,
                    0.6,
                    4,
                    3,
                    new OnMajorityPredictionsAggregator())
                    .withEnvironmentBuilder(LearningEnvironmentBuilder.defaultBuilder().withRNGSeed(1));

                System.out.println(">>> Perform evaluation of the model.");

                Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>()
                    .labeled(Vectorizer.LabelCoordinate.FIRST);

                double accuracy = Evaluator.evaluate(dataCache,
                    baggedTrainer.fit(ignite, dataCache, vectorizer),
                    vectorizer,
                    MetricName.ACCURACY
                );

                System.out.println(">>> ---------------------------------");

                System.out.println("\n>>> Accuracy " + accuracy);

                System.out.println(">>> Bagged logistic regression model over partitioned dataset usage example completed.");
            }
            finally {
                if (dataCache != null)
                    dataCache.destroy();
            }
        }
        finally {
            System.out.flush();
        }
    }
}
