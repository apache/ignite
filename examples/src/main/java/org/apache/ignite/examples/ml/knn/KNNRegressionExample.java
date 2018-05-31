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
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.knn.classification.KNNClassificationTrainer;
import org.apache.ignite.ml.knn.classification.KNNStrategy;
import org.apache.ignite.ml.knn.regression.KNNRegressionModel;
import org.apache.ignite.ml.knn.regression.KNNRegressionTrainer;
import org.apache.ignite.ml.math.distances.ManhattanDistance;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.thread.IgniteThread;

/**
 * Run kNN regression trainer over distributed dataset.
 *
 * @see KNNClassificationTrainer
 */
public class KNNRegressionExample {
    /** Run example. */
    public static void main(String[] args) throws InterruptedException {
        System.out.println();
        System.out.println(">>> kNN regression over cached dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                KNNRegressionExample.class.getSimpleName(), () -> {
                IgniteCache<Integer, double[]> dataCache = getTestCache(ignite);

                KNNRegressionTrainer trainer = new KNNRegressionTrainer();

                KNNRegressionModel knnMdl = (KNNRegressionModel) trainer.fit(
                    new CacheBasedDatasetBuilder<>(ignite, dataCache),
                    (k, v) -> Arrays.copyOfRange(v, 1, v.length),
                    (k, v) -> v[0]
                ).withK(5)
                    .withDistanceMeasure(new ManhattanDistance())
                    .withStrategy(KNNStrategy.WEIGHTED);

                int totalAmount = 0;
                // Calculate mean squared error (MSE)
                double mse = 0.0;
                // Calculate mean absolute error (MAE)
                double mae = 0.0;

                try (QueryCursor<Cache.Entry<Integer, double[]>> observations = dataCache.query(new ScanQuery<>())) {
                    for (Cache.Entry<Integer, double[]> observation : observations) {
                        double[] val = observation.getValue();
                        double[] inputs = Arrays.copyOfRange(val, 1, val.length);
                        double groundTruth = val[0];

                        double prediction = knnMdl.apply(new DenseLocalOnHeapVector(inputs));

                        mse += Math.pow(prediction - groundTruth, 2.0);
                        mae += Math.abs(prediction - groundTruth);

                        totalAmount++;
                    }

                    mse = mse / totalAmount;
                    System.out.println("\n>>> Mean squared error (MSE) " + mse);

                    mae = mae / totalAmount;
                    System.out.println("\n>>> Mean absolute error (MAE) " + mae);
                }
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

    /** The Iris dataset. */
    private static final double[][] data = {
        {199, 125, 256, 6000, 256, 16, 128},
        {253, 29, 8000, 32000, 32, 8, 32},
        {132, 29, 8000, 16000, 32, 8, 16},
        {290, 26, 8000, 32000, 64, 8, 32},
        {381, 23, 16000, 32000, 64, 16, 32},
        {749, 23, 16000, 64000, 64, 16, 32},
        {1238, 23, 32000, 64000, 128, 32, 64},
        {23, 400, 1000, 3000, 0, 1, 2},
        {24, 400, 512, 3500, 4, 1, 6},
        {70, 60, 2000, 8000, 65, 1, 8},
        {117, 50, 4000, 16000, 65, 1, 8},
        {15, 350, 64, 64, 0, 1, 4},
        {64, 200, 512, 16000, 0, 4, 32},
        {23, 167, 524, 2000, 8, 4, 15},
        {29, 143, 512, 5000, 0, 7, 32},
        {22, 143, 1000, 2000, 0, 5, 16},
        {124, 110, 5000, 5000, 142, 8, 64},
        {35, 143, 1500, 6300, 0, 5, 32},
        {39, 143, 3100, 6200, 0, 5, 20},
        {40, 143, 2300, 6200, 0, 6, 64},
        {45, 110, 3100, 6200, 0, 6, 64},
        {28, 320, 128, 6000, 0, 1, 12},
        {21, 320, 512, 2000, 4, 1, 3},
        {28, 320, 256, 6000, 0, 1, 6},
        {22, 320, 256, 3000, 4, 1, 3},
        {28, 320, 512, 5000, 4, 1, 5},
        {27, 320, 256, 5000, 4, 1, 6},
        {102, 25, 1310, 2620, 131, 12, 24},
        {74, 50, 2620, 10480, 30, 12, 24},
        {138, 56, 5240, 20970, 30, 12, 24},
        {136, 64, 5240, 20970, 30, 12, 24},
        {23, 50, 500, 2000, 8, 1, 4},
        {29, 50, 1000, 4000, 8, 1, 5},
        {44, 50, 2000, 8000, 8, 1, 5},
        {30, 50, 1000, 4000, 8, 3, 5},
        {41, 50, 1000, 8000, 8, 3, 5},
        {74, 50, 2000, 16000, 8, 3, 5},
        {54, 133, 1000, 12000, 9, 3, 12},
        {41, 133, 1000, 8000, 9, 3, 12},
        {18, 810, 512, 512, 8, 1, 1},
        {28, 810, 1000, 5000, 0, 1, 1},
        {36, 320, 512, 8000, 4, 1, 5},
        {38, 200, 512, 8000, 8, 1, 8},
        {34, 700, 384, 8000, 0, 1, 1},
        {19, 700, 256, 2000, 0, 1, 1},
        {72, 140, 1000, 16000, 16, 1, 3},
        {36, 200, 1000, 8000, 0, 1, 2},
        {30, 110, 1000, 4000, 16, 1, 2},
        {56, 110, 1000, 12000, 16, 1, 2},
        {42, 220, 1000, 8000, 16, 1, 2},
        {34, 800, 256, 8000, 0, 1, 4},
        {19, 125, 512, 1000, 0, 8, 20},
        {75, 75, 2000, 8000, 64, 1, 38},
        {113, 75, 2000, 16000, 64, 1, 38},
        {157, 75, 2000, 16000, 128, 1, 38},
        {18, 90, 256, 1000, 0, 3, 10},
        {20, 105, 256, 2000, 0, 3, 10},
        {28, 105, 1000, 4000, 0, 3, 24},
        {33, 105, 2000, 4000, 8, 3, 19},
        {47, 75, 2000, 8000, 8, 3, 24},
        {54, 75, 3000, 8000, 8, 3, 48},
        {20, 175, 256, 2000, 0, 3, 24},
        {23, 300, 768, 3000, 0, 6, 24},
        {25, 300, 768, 3000, 6, 6, 24},
        {52, 300, 768, 12000, 6, 6, 24},
        {27, 300, 768, 4500, 0, 1, 24},
        {50, 300, 384, 12000, 6, 1, 24},
        {18, 300, 192, 768, 6, 6, 24},
        {53, 180, 768, 12000, 6, 1, 31},
        {23, 330, 1000, 3000, 0, 2, 4},
        {30, 300, 1000, 4000, 8, 3, 64},
        {73, 300, 1000, 16000, 8, 2, 112},
        {20, 330, 1000, 2000, 0, 1, 2},
        {25, 330, 1000, 4000, 0, 3, 6},
        {28, 140, 2000, 4000, 0, 3, 6},
        {29, 140, 2000, 4000, 0, 4, 8},
        {32, 140, 2000, 4000, 8, 1, 20},
        {175, 140, 2000, 32000, 32, 1, 20},
        {57, 140, 2000, 8000, 32, 1, 54},
        {181, 140, 2000, 32000, 32, 1, 54},
        {32, 140, 2000, 4000, 8, 1, 20},
        {82, 57, 4000, 16000, 1, 6, 12},
        {171, 57, 4000, 24000, 64, 12, 16},
        {361, 26, 16000, 32000, 64, 16, 24},
        {350, 26, 16000, 32000, 64, 8, 24},
        {220, 26, 8000, 32000, 0, 8, 24},
        {113, 26, 8000, 16000, 0, 8, 16},
        {15, 480, 96, 512, 0, 1, 1},
        {21, 203, 1000, 2000, 0, 1, 5},
        {35, 115, 512, 6000, 16, 1, 6},
        {18, 1100, 512, 1500, 0, 1, 1},
        {20, 1100, 768, 2000, 0, 1, 1},
        {20, 600, 768, 2000, 0, 1, 1},
        {28, 400, 2000, 4000, 0, 1, 1},
        {45, 400, 4000, 8000, 0, 1, 1},
        {18, 900, 1000, 1000, 0, 1, 2},
        {17, 900, 512, 1000, 0, 1, 2},
        {26, 900, 1000, 4000, 4, 1, 2},
        {28, 900, 1000, 4000, 8, 1, 2},
        {28, 900, 2000, 4000, 0, 3, 6},
        {31, 225, 2000, 4000, 8, 3, 6},
        {42, 180, 2000, 8000, 8, 1, 6},
        {76, 185, 2000, 16000, 16, 1, 6},
        {76, 180, 2000, 16000, 16, 1, 6},
        {26, 225, 1000, 4000, 2, 3, 6},
        {59, 25, 2000, 12000, 8, 1, 4},
        {65, 25, 2000, 12000, 16, 3, 5},
        {101, 17, 4000, 16000, 8, 6, 12},
        {116, 17, 4000, 16000, 32, 6, 12},
        {18, 1500, 768, 1000, 0, 0, 0},
        {20, 1500, 768, 2000, 0, 0, 0},
        {20, 800, 768, 2000, 0, 0, 0},
        {30, 50, 2000, 4000, 0, 3, 6},
        {44, 50, 2000, 8000, 8, 3, 6},
        {82, 50, 2000, 16000, 24, 1, 6},
        {128, 50, 8000, 16000, 48, 1, 10},
        {37, 100, 1000, 8000, 0, 2, 6},
        {46, 100, 1000, 8000, 24, 2, 6},
        {46, 100, 1000, 8000, 24, 3, 6},
        {80, 50, 2000, 16000, 12, 3, 16},
        {88, 50, 2000, 16000, 24, 6, 16},
        {33, 150, 512, 4000, 0, 8, 128},
        {46, 115, 2000, 8000, 16, 1, 3},
        {29, 115, 2000, 4000, 2, 1, 5},
        {53, 92, 2000, 8000, 32, 1, 6},
        {41, 92, 2000, 8000, 4, 1, 6},
        {86, 75, 4000, 16000, 16, 1, 6},
        {95, 60, 4000, 16000, 32, 1, 6},
        {107, 60, 2000, 16000, 64, 5, 8},
        {117, 60, 4000, 16000, 64, 5, 8},
        {119, 50, 4000, 16000, 64, 5, 10},
        {120, 72, 4000, 16000, 64, 8, 16},
        {48, 72, 2000, 8000, 16, 6, 8},
        {126, 40, 8000, 16000, 32, 8, 16},
        {266, 40, 8000, 32000, 64, 8, 24},
        {270, 35, 8000, 32000, 64, 8, 24},
        {426, 38, 16000, 32000, 128, 16, 32},
        {151, 48, 4000, 24000, 32, 8, 24},
        {267, 38, 8000, 32000, 64, 8, 24},
        {603, 30, 16000, 32000, 256, 16, 24},
        {19, 112, 1000, 1000, 0, 1, 4},
        {21, 84, 1000, 2000, 0, 1, 6},
        {26, 56, 1000, 4000, 0, 1, 6},
        {35, 56, 2000, 6000, 0, 1, 8},
        {41, 56, 2000, 8000, 0, 1, 8},
        {47, 56, 4000, 8000, 0, 1, 8},
        {62, 56, 4000, 12000, 0, 1, 8},
        {78, 56, 4000, 16000, 0, 1, 8},
        {80, 38, 4000, 8000, 32, 16, 32},
        {142, 38, 8000, 16000, 64, 4, 8},
        {281, 38, 8000, 24000, 160, 4, 8},
        {190, 38, 4000, 16000, 128, 16, 32},
        {21, 200, 1000, 2000, 0, 1, 2},
        {25, 200, 1000, 4000, 0, 1, 4},
        {67, 200, 2000, 8000, 64, 1, 5},
        {24, 250, 512, 4000, 0, 1, 7},
        {24, 250, 512, 4000, 0, 4, 7},
        {64, 250, 1000, 16000, 1, 1, 8},
        {25, 160, 512, 4000, 2, 1, 5},
        {20, 160, 512, 2000, 2, 3, 8},
        {29, 160, 1000, 4000, 8, 1, 14},
        {43, 160, 1000, 8000, 16, 1, 14},
        {53, 160, 2000, 8000, 32, 1, 13},
        {19, 240, 512, 1000, 8, 1, 3},
        {22, 240, 512, 2000, 8, 1, 5},
        {31, 105, 2000, 4000, 8, 3, 8},
        {41, 105, 2000, 6000, 16, 6, 16},
        {47, 105, 2000, 8000, 16, 4, 14},
        {99, 52, 4000, 16000, 32, 4, 12},
        {67, 70, 4000, 12000, 8, 6, 8},
        {81, 59, 4000, 12000, 32, 6, 12},
        {149, 59, 8000, 16000, 64, 12, 24},
        {183, 26, 8000, 24000, 32, 8, 16},
        {275, 26, 8000, 32000, 64, 12, 16},
        {382, 26, 8000, 32000, 128, 24, 32},
        {56, 116, 2000, 8000, 32, 5, 28},
        {182, 50, 2000, 32000, 24, 6, 26},
        {227, 50, 2000, 32000, 48, 26, 52},
        {341, 50, 2000, 32000, 112, 52, 104},
        {360, 50, 4000, 32000, 112, 52, 104},
        {919, 30, 8000, 64000, 96, 12, 176},
        {978, 30, 8000, 64000, 128, 12, 176},
        {24, 180, 262, 4000, 0, 1, 3},
        {37, 124, 1000, 8000, 0, 1, 8},
        {50, 98, 1000, 8000, 32, 2, 8},
        {41, 125, 2000, 8000, 0, 2, 14},
        {47, 480, 512, 8000, 32, 0, 0},
        {25, 480, 1000, 4000, 0, 0, 0}
    };
}
