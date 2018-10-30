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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ml.util.TestCache;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.predictionsaggregator.OnMajorityPredictionsAggregator;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.regressions.logistic.binomial.LogisticRegressionSGDTrainer;
import org.apache.ignite.ml.selection.cv.CrossValidation;
import org.apache.ignite.ml.selection.scoring.metric.Accuracy;
import org.apache.ignite.ml.trainers.DatasetTrainer;
import org.apache.ignite.ml.trainers.transformers.TrainerTransformers;

import java.util.Arrays;

/**
 * This example shows how bagging technique may be applied to arbitrary trainer.
 * As an example (a bit synthetic) logistic regression is considered.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with test data points (based on the
 * <a href="https://en.wikipedia.org/wiki/Iris_flower_data_set"></a>Iris dataset</a>).</p>
 * <p>
 * After that it trains bootstrapped (or bagged) version of logistic regression trainer. Bootstrapping is done
 * on both samples and features (<a href="https://en.wikipedia.org/wiki/Bootstrap_aggregating"></a>Samples bagging</a>,
 * <a href="https://en.wikipedia.org/wiki/Random_subspace_method"></a>Features bagging</a>).</p>
 * <p>
 * Finally, this example applies cross-validation to resulted model and prints accuracy if each fold.</p>
 */
public class BaggedLogisticRegressionSGDTrainerExample {
    /** Run example. */
    public static void main(String[] args) throws InterruptedException {
        System.out.println();
        System.out.println(">>> Logistic regression model over partitioned dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, double[]> dataCache = new TestCache(ignite).fillCacheWith(data);

            System.out.println(">>> Create new logistic regression trainer object.");
            LogisticRegressionSGDTrainer<?> trainer = new LogisticRegressionSGDTrainer<>()
                .withUpdatesStgy(new UpdatesStrategy<>(
                    new SimpleGDUpdateCalculator(0.2),
                    SimpleGDParameterUpdate::sumLocal,
                    SimpleGDParameterUpdate::avg
                ))
                .withMaxIterations(100000)
                .withLocIterations(100)
                .withBatchSize(10)
                .withSeed(123L);

            System.out.println(">>> Perform the training to get the model.");

            DatasetTrainer< ModelsComposition, Double> baggedTrainer =
                TrainerTransformers.makeBagged(
                    trainer,
                    10,
                    0.6,
                    4,
                    3,
                    new OnMajorityPredictionsAggregator());

            double[] score = new CrossValidation<ModelsComposition, Double, Integer, double[]>().score(
                baggedTrainer,
                new Accuracy<>(),
                ignite,
                dataCache,
                (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
                (k, v) -> v[0],
                3
            );

            System.out.println(">>> ---------------------------------");

            Arrays.stream(score).forEach(sc -> {
                System.out.println("\n>>> Accuracy " + sc);
            });

            System.out.println(">>> Begged logistic regression model over partitioned dataset usage example completed.");
        }
    }

    /** The 1st and 2nd classes from the Iris dataset. */
    private static final double[][] data = {
        {0, 5.1, 3.5, 1.4, 0.2},
        {0, 4.9, 3, 1.4, 0.2},
        {0, 4.7, 3.2, 1.3, 0.2},
        {0, 4.6, 3.1, 1.5, 0.2},
        {0, 5, 3.6, 1.4, 0.2},
        {0, 5.4, 3.9, 1.7, 0.4},
        {0, 4.6, 3.4, 1.4, 0.3},
        {0, 5, 3.4, 1.5, 0.2},
        {0, 4.4, 2.9, 1.4, 0.2},
        {0, 4.9, 3.1, 1.5, 0.1},
        {0, 5.4, 3.7, 1.5, 0.2},
        {0, 4.8, 3.4, 1.6, 0.2},
        {0, 4.8, 3, 1.4, 0.1},
        {0, 4.3, 3, 1.1, 0.1},
        {0, 5.8, 4, 1.2, 0.2},
        {0, 5.7, 4.4, 1.5, 0.4},
        {0, 5.4, 3.9, 1.3, 0.4},
        {0, 5.1, 3.5, 1.4, 0.3},
        {0, 5.7, 3.8, 1.7, 0.3},
        {0, 5.1, 3.8, 1.5, 0.3},
        {0, 5.4, 3.4, 1.7, 0.2},
        {0, 5.1, 3.7, 1.5, 0.4},
        {0, 4.6, 3.6, 1, 0.2},
        {0, 5.1, 3.3, 1.7, 0.5},
        {0, 4.8, 3.4, 1.9, 0.2},
        {0, 5, 3, 1.6, 0.2},
        {0, 5, 3.4, 1.6, 0.4},
        {0, 5.2, 3.5, 1.5, 0.2},
        {0, 5.2, 3.4, 1.4, 0.2},
        {0, 4.7, 3.2, 1.6, 0.2},
        {0, 4.8, 3.1, 1.6, 0.2},
        {0, 5.4, 3.4, 1.5, 0.4},
        {0, 5.2, 4.1, 1.5, 0.1},
        {0, 5.5, 4.2, 1.4, 0.2},
        {0, 4.9, 3.1, 1.5, 0.1},
        {0, 5, 3.2, 1.2, 0.2},
        {0, 5.5, 3.5, 1.3, 0.2},
        {0, 4.9, 3.1, 1.5, 0.1},
        {0, 4.4, 3, 1.3, 0.2},
        {0, 5.1, 3.4, 1.5, 0.2},
        {0, 5, 3.5, 1.3, 0.3},
        {0, 4.5, 2.3, 1.3, 0.3},
        {0, 4.4, 3.2, 1.3, 0.2},
        {0, 5, 3.5, 1.6, 0.6},
        {0, 5.1, 3.8, 1.9, 0.4},
        {0, 4.8, 3, 1.4, 0.3},
        {0, 5.1, 3.8, 1.6, 0.2},
        {0, 4.6, 3.2, 1.4, 0.2},
        {0, 5.3, 3.7, 1.5, 0.2},
        {0, 5, 3.3, 1.4, 0.2},
        {1, 7, 3.2, 4.7, 1.4},
        {1, 6.4, 3.2, 4.5, 1.5},
        {1, 6.9, 3.1, 4.9, 1.5},
        {1, 5.5, 2.3, 4, 1.3},
        {1, 6.5, 2.8, 4.6, 1.5},
        {1, 5.7, 2.8, 4.5, 1.3},
        {1, 6.3, 3.3, 4.7, 1.6},
        {1, 4.9, 2.4, 3.3, 1},
        {1, 6.6, 2.9, 4.6, 1.3},
        {1, 5.2, 2.7, 3.9, 1.4},
        {1, 5, 2, 3.5, 1},
        {1, 5.9, 3, 4.2, 1.5},
        {1, 6, 2.2, 4, 1},
        {1, 6.1, 2.9, 4.7, 1.4},
        {1, 5.6, 2.9, 3.6, 1.3},
        {1, 6.7, 3.1, 4.4, 1.4},
        {1, 5.6, 3, 4.5, 1.5},
        {1, 5.8, 2.7, 4.1, 1},
        {1, 6.2, 2.2, 4.5, 1.5},
        {1, 5.6, 2.5, 3.9, 1.1},
        {1, 5.9, 3.2, 4.8, 1.8},
        {1, 6.1, 2.8, 4, 1.3},
        {1, 6.3, 2.5, 4.9, 1.5},
        {1, 6.1, 2.8, 4.7, 1.2},
        {1, 6.4, 2.9, 4.3, 1.3},
        {1, 6.6, 3, 4.4, 1.4},
        {1, 6.8, 2.8, 4.8, 1.4},
        {1, 6.7, 3, 5, 1.7},
        {1, 6, 2.9, 4.5, 1.5},
        {1, 5.7, 2.6, 3.5, 1},
        {1, 5.5, 2.4, 3.8, 1.1},
        {1, 5.5, 2.4, 3.7, 1},
        {1, 5.8, 2.7, 3.9, 1.2},
        {1, 6, 2.7, 5.1, 1.6},
        {1, 5.4, 3, 4.5, 1.5},
        {1, 6, 3.4, 4.5, 1.6},
        {1, 6.7, 3.1, 4.7, 1.5},
        {1, 6.3, 2.3, 4.4, 1.3},
        {1, 5.6, 3, 4.1, 1.3},
        {1, 5.5, 2.5, 4, 1.3},
        {1, 5.5, 2.6, 4.4, 1.2},
        {1, 6.1, 3, 4.6, 1.4},
        {1, 5.8, 2.6, 4, 1.2},
        {1, 5, 2.3, 3.3, 1},
        {1, 5.6, 2.7, 4.2, 1.3},
        {1, 5.7, 3, 4.2, 1.2},
        {1, 5.7, 2.9, 4.2, 1.3},
        {1, 6.2, 2.9, 4.3, 1.3},
        {1, 5.1, 2.5, 3, 1.1},
        {1, 5.7, 2.8, 4.1, 1.3},
    };
}
