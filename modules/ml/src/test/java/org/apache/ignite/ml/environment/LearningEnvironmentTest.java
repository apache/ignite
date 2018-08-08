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

package org.apache.ignite.ml.environment;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.environment.logging.ConsoleLogger;
import org.apache.ignite.ml.environment.logging.MLLogger;
import org.apache.ignite.ml.environment.parallelism.ParallelismStrategy;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.tree.randomforest.RandomForestRegressionTrainer;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.thread.IgniteThread;

/**
 * Tests for {@link LearningEnvironment} that require to start the whole Ignite infrastructure. IMPL NOTE based on
 * RandomForestRegressionExample example.
 */
public class LearningEnvironmentTest extends GridCommonAbstractTest {
    /** Number of nodes in grid */
    private static final int NODE_COUNT = 1;

    /** Ignite instance. */
    private Ignite ignite;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        for (int i = 1; i <= NODE_COUNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() {
        stopAllGrids();
    }

    /**
     * {@inheritDoc}
     */
    @Override protected void beforeTest() {
        /* Grid instance. */
        ignite = grid(NODE_COUNT);
        ignite.configuration().setPeerClassLoadingEnabled(true);
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
    }

    /** */
    public void testBasic() throws InterruptedException {
        AtomicReference<Integer> actualAmount = new AtomicReference<>(null);
        AtomicReference<Double> actualMse = new AtomicReference<>(null);
        AtomicReference<Double> actualMae = new AtomicReference<>(null);

        IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
            LearningEnvironmentTest.class.getSimpleName(), () -> {
            IgniteCache<Integer, double[]> dataCache = getTestCache(ignite);

            RandomForestRegressionTrainer trainer = new RandomForestRegressionTrainer(13, 4, 101, 0.3, 2, 0);

            trainer.setEnvironment(LearningEnvironment.builder()
                .withParallelismStrategy(ParallelismStrategy.Type.ON_DEFAULT_POOL)
                .withLoggingFactory(ConsoleLogger.factory(MLLogger.VerboseLevel.LOW))
                .build()
            );

            ModelsComposition randomForest = trainer.fit(ignite, dataCache,
                (k, v) -> VectorUtils.of(Arrays.copyOfRange(v, 0, v.length - 1)),
                (k, v) -> v[v.length - 1]
            );

            double mse = 0.0;
            double mae = 0.0;
            int totalAmount = 0;

            try (QueryCursor<Cache.Entry<Integer, double[]>> observations = dataCache.query(new ScanQuery<>())) {
                for (Cache.Entry<Integer, double[]> observation : observations) {
                    double difference = estimatePrediction(randomForest, observation);

                    mse += Math.pow(difference, 2.0);
                    mae += Math.abs(difference);

                    totalAmount++;
                }
            }

            actualAmount.set(totalAmount);

            mse = mse / totalAmount;
            actualMse.set(mse);

            mae = mae / totalAmount;
            actualMae.set(mae);
        });

        igniteThread.start();
        igniteThread.join();

        assertEquals("Total amount", 23, (int)actualAmount.get());
        assertTrue("Mean squared error (MSE)", actualMse.get() > 0);
        assertTrue("Mean absolute error (MAE)", actualMae.get() > 0);
    }

    /** */
    private double estimatePrediction(ModelsComposition randomForest, Cache.Entry<Integer, double[]> observation) {
        double[] val = observation.getValue();
        double[] inputs = Arrays.copyOfRange(val, 0, val.length - 1);
        double groundTruth = val[val.length - 1];

        double prediction = randomForest.apply(VectorUtils.of(inputs));

        return prediction - groundTruth;
    }

    /**
     * Fills cache with data and returns it.
     *
     * @param ignite Ignite instance.
     * @return Filled Ignite Cache.
     */
    private IgniteCache<Integer, double[]> getTestCache(Ignite ignite) {
        CacheConfiguration<Integer, double[]> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setName(UUID.randomUUID().toString());
        cacheConfiguration.setAffinity(new RendezvousAffinityFunction(false, 10));

        IgniteCache<Integer, double[]> cache = ignite.createCache(cacheConfiguration);

        for (int i = 0; i < data.length; i++)
            cache.put(i, data[i]);

        return cache;
    }

    /**
     * Part of the Boston housing dataset.
     */
    private static final double[][] data = {
        {0.02731,0.00,7.070,0,0.4690,6.4210,78.90,4.9671,2,242.0,17.80,396.90,9.14,21.60},
        {0.02729,0.00,7.070,0,0.4690,7.1850,61.10,4.9671,2,242.0,17.80,392.83,4.03,34.70},
        {0.03237,0.00,2.180,0,0.4580,6.9980,45.80,6.0622,3,222.0,18.70,394.63,2.94,33.40},
        {0.06905,0.00,2.180,0,0.4580,7.1470,54.20,6.0622,3,222.0,18.70,396.90,5.33,36.20},
        {0.02985,0.00,2.180,0,0.4580,6.4300,58.70,6.0622,3,222.0,18.70,394.12,5.21,28.70},
        {0.08829,12.50,7.870,0,0.5240,6.0120,66.60,5.5605,5,311.0,15.20,395.60,12.43,22.90},
        {0.14455,12.50,7.870,0,0.5240,6.1720,96.10,5.9505,5,311.0,15.20,396.90,19.15,27.10},
        {0.21124,12.50,7.870,0,0.5240,5.6310,100.00,6.0821,5,311.0,15.20,386.63,29.93,16.50},
        {0.17004,12.50,7.870,0,0.5240,6.0040,85.90,6.5921,5,311.0,15.20,386.71,17.10,18.90},
        {0.22489,12.50,7.870,0,0.5240,6.3770,94.30,6.3467,5,311.0,15.20,392.52,20.45,15.00},
        {0.11747,12.50,7.870,0,0.5240,6.0090,82.90,6.2267,5,311.0,15.20,396.90,13.27,18.90},
        {0.09378,12.50,7.870,0,0.5240,5.8890,39.00,5.4509,5,311.0,15.20,390.50,15.71,21.70},
        {0.62976,0.00,8.140,0,0.5380,5.9490,61.80,4.7075,4,307.0,21.00,396.90,8.26,20.40},
        {0.63796,0.00,8.140,0,0.5380,6.0960,84.50,4.4619,4,307.0,21.00,380.02,10.26,18.20},
        {0.62739,0.00,8.140,0,0.5380,5.8340,56.50,4.4986,4,307.0,21.00,395.62,8.47,19.90},
        {1.05393,0.00,8.140,0,0.5380,5.9350,29.30,4.4986,4,307.0,21.00,386.85,6.58,23.10},
        {0.78420,0.00,8.140,0,0.5380,5.9900,81.70,4.2579,4,307.0,21.00,386.75,14.67,17.50},
        {0.80271,0.00,8.140,0,0.5380,5.4560,36.60,3.7965,4,307.0,21.00,288.99,11.69,20.20},
        {0.72580,0.00,8.140,0,0.5380,5.7270,69.50,3.7965,4,307.0,21.00,390.95,11.28,18.20},
        {1.25179,0.00,8.140,0,0.5380,5.5700,98.10,3.7979,4,307.0,21.00,376.57,21.02,13.60},
        {0.85204,0.00,8.140,0,0.5380,5.9650,89.20,4.0123,4,307.0,21.00,392.53,13.83,19.60},
        {1.23247,0.00,8.140,0,0.5380,6.1420,91.70,3.9769,4,307.0,21.00,396.90,18.72,15.20},
        {0.98843,0.00,8.140,0,0.5380,5.8130,100.00,4.0952,4,307.0,21.00,394.54,19.88,14.50}
    };

}

