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

package org.apache.ignite.examples.ml.regression.logistic.binary;

import java.io.FileNotFoundException;
import java.util.Arrays;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.examples.ml.util.MLSandboxDatasets;
import org.apache.ignite.examples.ml.util.SandboxMLCache;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionModel;
import org.apache.ignite.ml.regressions.logistic.LogisticRegressionSGDTrainer;

/**
 * Run logistic regression model based on <a href="https://en.wikipedia.org/wiki/Stochastic_gradient_descent">
 * stochastic gradient descent</a> algorithm ({@link LogisticRegressionSGDTrainer}) over distributed cache.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with test data points (based on the
 * <a href="https://en.wikipedia.org/wiki/Iris_flower_data_set"></a>Iris dataset</a>).</p>
 * <p>
 * After that it trains the logistic regression model based on the specified data.</p>
 * <p>
 * Finally, this example loops over the test set of data points, applies the trained model to predict the target value,
 * compares prediction to expected outcome (ground truth), and builds
 * <a href="https://en.wikipedia.org/wiki/Confusion_matrix">confusion matrix</a>.</p>
 * <p>
 * You can change the test data used in this example and re-run it to explore this algorithm further.</p>
 */
public class LogisticRegressionSGDTrainerExample {
    /** Run example. */
    public static void main(String[] args) throws FileNotFoundException {
        System.out.println();
        System.out.println(">>> Logistic regression model over partitioned dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = new SandboxMLCache(ignite)
                .fillCacheWith(MLSandboxDatasets.TWO_CLASSED_IRIS);

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
            LogisticRegressionModel mdl = trainer.fit(
                ignite,
                dataCache,
                (k, v) -> v.copyOfRange(1, v.size()),
                (k, v) -> v.get(0)
            );

            System.out.println(">>> Logistic regression model: " + mdl);

            int amountOfErrors = 0;
            int totalAmount = 0;

            // Build confusion matrix. See https://en.wikipedia.org/wiki/Confusion_matrix
            int[][] confusionMtx = {{0, 0}, {0, 0}};

            try (QueryCursor<Cache.Entry<Integer, Vector>> observations = dataCache.query(new ScanQuery<>())) {
                for (Cache.Entry<Integer, Vector> observation : observations) {
                    Vector val = observation.getValue();
                    Vector inputs = val.copyOfRange(1, val.size());
                    double groundTruth = val.get(0);

                    double prediction = mdl.apply(inputs);

                    totalAmount++;
                    if(groundTruth != prediction)
                        amountOfErrors++;

                    int idx1 = (int)prediction;
                    int idx2 = (int)groundTruth;

                    confusionMtx[idx1][idx2]++;

                    System.out.printf(">>> | %.4f\t\t| %.4f\t\t|\n", prediction, groundTruth);
                }

                System.out.println(">>> ---------------------------------");

                System.out.println("\n>>> Absolute amount of errors " + amountOfErrors);
                System.out.println("\n>>> Accuracy " + (1 - amountOfErrors / (double)totalAmount));
            }

            System.out.println("\n>>> Confusion matrix is " + Arrays.deepToString(confusionMtx));
            System.out.println(">>> ---------------------------------");

            System.out.println(">>> Logistic regression model over partitioned dataset usage example completed.");
        }
    }
}
