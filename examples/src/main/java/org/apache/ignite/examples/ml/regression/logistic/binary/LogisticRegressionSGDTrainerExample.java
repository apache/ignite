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

import java.util.Arrays;
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.nn.UpdatesStrategy;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.regressions.logistic.binomial.LogisticRegressionModel;
import org.apache.ignite.ml.regressions.logistic.binomial.LogisticRegressionSGDTrainer;
import org.apache.ignite.thread.IgniteThread;

/**
 * Run logistic regression model over distributed cache.
 *
 * @see LogisticRegressionSGDTrainer
 */
public class LogisticRegressionSGDTrainerExample {
    /** Run example. */
    public static void main(String[] args) throws InterruptedException {
        System.out.println();
        System.out.println(">>> Logistic regression model over partitioned dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");
            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                LogisticRegressionSGDTrainerExample.class.getSimpleName(), () -> {

                IgniteCache<Integer, double[]> dataCache = getTestCache(ignite);

                System.out.println(">>> Create new logistic regression trainer object.");
                LogisticRegressionSGDTrainer<?> trainer = new LogisticRegressionSGDTrainer<>(new UpdatesStrategy<>(
                    new SimpleGDUpdateCalculator(0.2),
                    SimpleGDParameterUpdate::sumLocal,
                    SimpleGDParameterUpdate::avg
                ), 100000,  10, 100, 123L);

                System.out.println(">>> Perform the training to get the model.");
                LogisticRegressionModel mdl = trainer.fit(
                    ignite,
                    dataCache,
                    (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 1, v.length)),
                    (k, v) -> v[0]
                ).withRawLabels(true);

                System.out.println(">>> Logistic regression model: " + mdl);

                int amountOfErrors = 0;
                int totalAmount = 0;

                // Build confusion matrix. See https://en.wikipedia.org/wiki/Confusion_matrix
                int[][] confusionMtx = {{0, 0}, {0, 0}};

                try (QueryCursor<Cache.Entry<Integer, double[]>> observations = dataCache.query(new ScanQuery<>())) {
                    for (Cache.Entry<Integer, double[]> observation : observations) {
                        double[] val = observation.getValue();
                        double[] inputs = Arrays.copyOfRange(val, 1, val.length);
                        double groundTruth = val[0];

                        double prediction = mdl.apply(new DenseVector(inputs));

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
            });

            igniteThread.start();

            igniteThread.join();
        }
    }
    /**
     * Fills cache with data and returns it.
     *
     * @param ignite Ignite instance.
     * @return Filled Ignite Cache.
     */
    private static IgniteCache<Integer, double[]> getTestCache(Ignite ignite) {
        CacheConfiguration<Integer, double[]> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName("TEST_" + UUID.randomUUID());
        cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 10));

        IgniteCache<Integer, double[]> cache = ignite.createCache(cacheConfiguration);

        for (int i = 0; i < data.length; i++)
            cache.put(i, data[i]);

        return cache;
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
