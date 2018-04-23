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

package org.apache.ignite.examples.ml.svm.binary;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.svm.SVMLinearBinaryClassificationModel;
import org.apache.ignite.ml.svm.SVMLinearBinaryClassificationTrainer;
import org.apache.ignite.thread.IgniteThread;

import javax.cache.Cache;
import java.util.Arrays;
import java.util.UUID;

/**
 * Run SVM binary-class classification model over distributed dataset.
 *
 * @see SVMLinearBinaryClassificationModel
 */
public class SVMBinaryClassificationExample {
    /** Run example. */
    public static void main(String[] args) throws InterruptedException {
        System.out.println();
        System.out.println(">>> SVM Binary classification model over cached dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                SVMBinaryClassificationExample.class.getSimpleName(), () -> {
                IgniteCache<Integer, double[]> dataCache = getTestCache(ignite);

                SVMLinearBinaryClassificationTrainer trainer = new SVMLinearBinaryClassificationTrainer();

                SVMLinearBinaryClassificationModel mdl = trainer.fit(
                    ignite,
                    dataCache,
                    (k, v) -> Arrays.copyOfRange(v, 1, v.length),
                    (k, v) -> v[0]
                );

                System.out.println(">>> SVM model " + mdl);

                System.out.println(">>> ---------------------------------");
                System.out.println(">>> | Prediction\t| Ground Truth\t|");
                System.out.println(">>> ---------------------------------");

                int amountOfErrors = 0;
                int totalAmount = 0;

                // Build confusion matrix. See https://en.wikipedia.org/wiki/Confusion_matrix
                int[][] confusionMtx = {{0, 0}, {0, 0}};

                try (QueryCursor<Cache.Entry<Integer, double[]>> observations = dataCache.query(new ScanQuery<>())) {
                    for (Cache.Entry<Integer, double[]> observation : observations) {
                        double[] val = observation.getValue();
                        double[] inputs = Arrays.copyOfRange(val, 1, val.length);
                        double groundTruth = val[0];

                        double prediction = mdl.apply(new DenseLocalOnHeapVector(inputs));

                        totalAmount++;
                        if(groundTruth != prediction)
                            amountOfErrors++;

                        int idx1 = (int)prediction == -1.0 ? 0 : 1;
                        int idx2 = (int)groundTruth == -1.0 ? 0 : 1;

                        confusionMtx[idx1][idx2]++;

                        System.out.printf(">>> | %.4f\t\t| %.4f\t\t|\n", prediction, groundTruth);
                    }

                    System.out.println(">>> ---------------------------------");

                    System.out.println("\n>>> Absolute amount of errors " + amountOfErrors);
                    System.out.println("\n>>> Accuracy " + (1 - amountOfErrors / (double)totalAmount));
                }

                System.out.println("\n>>> Confusion matrix is " + Arrays.deepToString(confusionMtx));
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
        {-1, 5.1, 3.5, 1.4, 0.2},
        {-1, 4.9, 3, 1.4, 0.2},
        {-1, 4.7, 3.2, 1.3, 0.2},
        {-1, 4.6, 3.1, 1.5, 0.2},
        {-1, 5, 3.6, 1.4, 0.2},
        {-1, 5.4, 3.9, 1.7, 0.4},
        {-1, 4.6, 3.4, 1.4, 0.3},
        {-1, 5, 3.4, 1.5, 0.2},
        {-1, 4.4, 2.9, 1.4, 0.2},
        {-1, 4.9, 3.1, 1.5, 0.1},
        {-1, 5.4, 3.7, 1.5, 0.2},
        {-1, 4.8, 3.4, 1.6, 0.2},
        {-1, 4.8, 3, 1.4, 0.1},
        {-1, 4.3, 3, 1.1, 0.1},
        {-1, 5.8, 4, 1.2, 0.2},
        {-1, 5.7, 4.4, 1.5, 0.4},
        {-1, 5.4, 3.9, 1.3, 0.4},
        {-1, 5.1, 3.5, 1.4, 0.3},
        {-1, 5.7, 3.8, 1.7, 0.3},
        {-1, 5.1, 3.8, 1.5, 0.3},
        {-1, 5.4, 3.4, 1.7, 0.2},
        {-1, 5.1, 3.7, 1.5, 0.4},
        {-1, 4.6, 3.6, 1, 0.2},
        {-1, 5.1, 3.3, 1.7, 0.5},
        {-1, 4.8, 3.4, 1.9, 0.2},
        {-1, 5, 3, 1.6, 0.2},
        {-1, 5, 3.4, 1.6, 0.4},
        {-1, 5.2, 3.5, 1.5, 0.2},
        {-1, 5.2, 3.4, 1.4, 0.2},
        {-1, 4.7, 3.2, 1.6, 0.2},
        {-1, 4.8, 3.1, 1.6, 0.2},
        {-1, 5.4, 3.4, 1.5, 0.4},
        {-1, 5.2, 4.1, 1.5, 0.1},
        {-1, 5.5, 4.2, 1.4, 0.2},
        {-1, 4.9, 3.1, 1.5, 0.1},
        {-1, 5, 3.2, 1.2, 0.2},
        {-1, 5.5, 3.5, 1.3, 0.2},
        {-1, 4.9, 3.1, 1.5, 0.1},
        {-1, 4.4, 3, 1.3, 0.2},
        {-1, 5.1, 3.4, 1.5, 0.2},
        {-1, 5, 3.5, 1.3, 0.3},
        {-1, 4.5, 2.3, 1.3, 0.3},
        {-1, 4.4, 3.2, 1.3, 0.2},
        {-1, 5, 3.5, 1.6, 0.6},
        {-1, 5.1, 3.8, 1.9, 0.4},
        {-1, 4.8, 3, 1.4, 0.3},
        {-1, 5.1, 3.8, 1.6, 0.2},
        {-1, 4.6, 3.2, 1.4, 0.2},
        {-1, 5.3, 3.7, 1.5, 0.2},
        {-1, 5, 3.3, 1.4, 0.2},
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
