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

package org.apache.ignite.examples.ml.nn;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.ml.dataset.impl.cache.CacheBasedDatasetBuilder;
import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.nn.Activators;
import org.apache.ignite.ml.nn.MLPTrainer;
import org.apache.ignite.ml.nn.MultilayerPerceptron;
import org.apache.ignite.ml.nn.architecture.MLPArchitecture;
import org.apache.ignite.ml.optimization.LossFunctions;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDParameterUpdate;
import org.apache.ignite.ml.optimization.updatecalculators.SimpleGDUpdateCalculator;
import org.apache.ignite.ml.trainers.group.UpdatesStrategy;
import org.apache.ignite.thread.IgniteThread;

/**
 * Example of using distributed {@link MultilayerPerceptron}.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.</p>
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-ignite.xml} configuration.</p>
 */
public class MLPTrainerExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws InterruptedException {
        // IMPL NOTE based on MLPGroupTrainerTest#testXOR
        System.out.println(">>> Distributed multilayer perceptron example started.");

        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            // Create IgniteThread, we must work with SparseDistributedMatrix inside IgniteThread
            // because we create ignite cache internally.
            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                MLPTrainerExample.class.getSimpleName(), () -> {

                // Create cache with training data.
                CacheConfiguration<Integer, LabeledPoint> trainingSetCfg = new CacheConfiguration<>();
                trainingSetCfg.setName("TRAINING_SET");
                trainingSetCfg.setAffinity(new RendezvousAffinityFunction(false, 10));

                IgniteCache<Integer, LabeledPoint> trainingSet = ignite.createCache(trainingSetCfg);

                // Fill cache with training data.
                trainingSet.put(0, new LabeledPoint(0, 0, 0));
                trainingSet.put(1, new LabeledPoint(0, 1, 1));
                trainingSet.put(2, new LabeledPoint(1, 0, 1));
                trainingSet.put(3, new LabeledPoint(1, 1, 0));

                // Define a layered architecture.
                MLPArchitecture arch = new MLPArchitecture(2).
                    withAddedLayer(10, true, Activators.RELU).
                    withAddedLayer(1, false, Activators.SIGMOID);

                // Define a neural network trainer.
                MLPTrainer<SimpleGDParameterUpdate> trainer = new MLPTrainer<>(
                    arch,
                    LossFunctions.MSE,
                    new UpdatesStrategy<>(
                        new SimpleGDUpdateCalculator(0.1),
                        SimpleGDParameterUpdate::sumLocal,
                        SimpleGDParameterUpdate::avg
                    ),
                    3000,
                    4,
                    50,
                    123L
                );

                // Train neural network and get multilayer perceptron model.
                MultilayerPerceptron mlp = trainer.fit(
                    new CacheBasedDatasetBuilder<>(ignite, trainingSet),
                    (k, v) -> new double[] {v.x, v.y},
                    (k, v) -> new double[] {v.lb}
                );

                int totalCnt = 4;
                int failCnt = 0;

                // Calculate score.
                for (int i = 0; i < 4; i++) {
                    LabeledPoint pnt = trainingSet.get(i);
                    Matrix predicted = mlp.apply(new DenseLocalOnHeapMatrix(new double[][] {{pnt.x, pnt.y}}));
                    failCnt += Math.abs(predicted.get(0, 0) - pnt.lb) < 0.5 ? 0 : 1;
                }

                double failRatio = (double)failCnt / totalCnt;

                System.out.println("\n>>> Fail percentage: " + (failRatio * 100) + "%.");

                System.out.println("\n>>> Distributed multilayer perceptron example completed.");
            });

            igniteThread.start();

            igniteThread.join();
        }
    }

    /** Point data class. */
    private static class Point {
        /** X coordinate. */
        final double x;

        /** Y coordinate. */
        final double y;

        /**
         * Constructs a new instance of point.
         *
         * @param x X coordinate.
         * @param y Y coordinate.
         */
        Point(double x, double y) {
            this.x = x;
            this.y = y;
        }
    }

    /** Labeled point data class. */
    private static class LabeledPoint extends Point {
        /** Point label. */
        final double lb;

        /**
         * Constructs a new instance of labeled point data.
         *
         * @param x X coordinate.
         * @param y Y coordinate.
         * @param lb Point label.
         */
        LabeledPoint(double x, double y, double lb) {
            super(x, y);
            this.lb = lb;
        }
    }
}
