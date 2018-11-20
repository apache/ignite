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

package org.apache.ignite.examples.ml.knn;

import java.io.FileNotFoundException;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.examples.ml.util.MLSandboxDatasets;
import org.apache.ignite.examples.ml.util.SandboxMLCache;
import org.apache.ignite.ml.knn.classification.NNStrategy;
import org.apache.ignite.ml.knn.regression.KNNRegressionModel;
import org.apache.ignite.ml.knn.regression.KNNRegressionTrainer;
import org.apache.ignite.ml.math.distances.ManhattanDistance;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Run kNN regression trainer ({@link KNNRegressionTrainer}) over distributed dataset.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with test data points (based on the
 * <a href="https://en.wikipedia.org/wiki/Iris_flower_data_set"></a>Iris dataset</a>).</p>
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
public class KNNRegressionExample {
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

            KNNRegressionModel knnMdl = (KNNRegressionModel) trainer.fit(
                ignite,
                dataCache,
                (k, v) -> v.copyOfRange(1, v.size()),
                (k, v) -> v.get(0)
            ).withK(5)
                .withDistanceMeasure(new ManhattanDistance())
                .withStrategy(NNStrategy.WEIGHTED);

            System.out.println(">>> ---------------------------------");
            System.out.println(">>> | Prediction\t| Ground Truth\t|");
            System.out.println(">>> ---------------------------------");

            int totalAmount = 0;
            // Calculate mean squared error (MSE)
            double mse = 0.0;
            // Calculate mean absolute error (MAE)
            double mae = 0.0;

            try (QueryCursor<Cache.Entry<Integer, Vector>> observations = dataCache.query(new ScanQuery<>())) {
                for (Cache.Entry<Integer, Vector> observation : observations) {
                    Vector val = observation.getValue();
                    Vector inputs = val.copyOfRange(1, val.size());
                    double groundTruth = val.get(0);

                    double prediction = knnMdl.apply(inputs);

                    mse += Math.pow(prediction - groundTruth, 2.0);
                    mae += Math.abs(prediction - groundTruth);

                    totalAmount++;

                    System.out.printf(">>> | %.4f\t\t| %.4f\t\t|\n", prediction, groundTruth);
                }

                System.out.println(">>> ---------------------------------");

                mse = mse / totalAmount;
                System.out.println("\n>>> Mean squared error (MSE) " + mse);

                mae = mae / totalAmount;
                System.out.println("\n>>> Mean absolute error (MAE) " + mae);

                System.out.println(">>> kNN regression over cached dataset usage example completed.");
            }
        }
    }
}
