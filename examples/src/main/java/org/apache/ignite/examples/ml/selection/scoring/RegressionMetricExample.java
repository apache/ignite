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

package org.apache.ignite.examples.ml.selection.scoring;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.knn.classification.NNStrategy;
import org.apache.ignite.ml.knn.regression.KNNRegressionModel;
import org.apache.ignite.ml.knn.regression.KNNRegressionTrainer;
import org.apache.ignite.ml.math.distances.ManhattanDistance;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.regression.RegressionMetricValues;
import org.apache.ignite.ml.selection.scoring.metric.regression.RegressionMetrics;
import org.apache.ignite.ml.util.MLSandboxDatasets;
import org.apache.ignite.ml.util.SandboxMLCache;

import java.io.FileNotFoundException;

/**
 * Run kNN regression trainer ({@link KNNRegressionTrainer}) over distributed dataset.
 * <p>
 * After that it trains the model based on the specified data using
 * <a href="https://en.wikipedia.org/wiki/K-nearest_neighbors_algorithm">kNN</a> regression algorithm.</p>
 * <p>
 * Finally, this example loops over the test set of data points, applies the trained model to predict what cluster
 * does this point belong to, and compares prediction to expected outcome (ground truth).</p>
 * <p>
 * You can change the test data used in this example or trainer object settings and re-run it to explore
 * this algorithm further.</p>
 */
public class RegressionMetricExample {
    /** Run example. */
    public static void main(String[] args) throws FileNotFoundException {
        System.out.println();
        System.out.println(">>> kNN regression over cached dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = new SandboxMLCache(ignite)
                .fillCacheWith(MLSandboxDatasets.CLEARED_MACHINES);

            KNNRegressionTrainer trainer = new KNNRegressionTrainer();

            final IgniteBiFunction<Integer, Vector, Vector> featureExtractor = (k, v) -> v.copyOfRange(1, v.size());
            final IgniteBiFunction<Integer, Vector, Double> lbExtractor = (k, v) -> v.get(0);

            KNNRegressionModel knnMdl = (KNNRegressionModel) trainer.fit(
                ignite,
                dataCache,
                featureExtractor,
                lbExtractor
            ).withK(5)
                .withDistanceMeasure(new ManhattanDistance())
                .withStrategy(NNStrategy.WEIGHTED);


            double mae = Evaluator.evaluate(
                dataCache,
                knnMdl,
                featureExtractor,
                lbExtractor,
                new RegressionMetrics().withMetric(RegressionMetricValues::mae)
            );

            System.out.println("\n>>> Mae " + mae);
        }
    }
}
