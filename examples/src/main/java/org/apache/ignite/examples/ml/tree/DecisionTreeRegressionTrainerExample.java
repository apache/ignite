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

package org.apache.ignite.examples.ml.tree;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.ignite.ml.tree.DecisionTreeRegressionTrainer;

/**
 * Example of using distributed {@link DecisionTreeRegressionTrainer}.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with generated test data points ({@code sin(x)}
 * on interval {@code [0, 10)}).</p>
 * <p>
 * After that it creates classification trainer and uses it to train the model on the training set.</p>
 * <p>
 * Finally, this example loops over the test data points, applies the trained model, and compares prediction
 * to expected outcome (ground truth).</p>
 * <p>
 * You can change the test data used in this example and re-run it to explore this algorithm further.</p>
 */
public class DecisionTreeRegressionTrainerExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String... args) throws InterruptedException {
        System.out.println(">>> Decision tree regression trainer example started.");

        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            // Create cache with training data.
            CacheConfiguration<Integer, Point> trainingSetCfg = new CacheConfiguration<>();
            trainingSetCfg.setName("TRAINING_SET");
            trainingSetCfg.setAffinity(new RendezvousAffinityFunction(false, 10));

            IgniteCache<Integer, Point> trainingSet = ignite.createCache(trainingSetCfg);

            // Fill training data.
            generatePoints(trainingSet);

            // Create regression trainer.
            DecisionTreeRegressionTrainer trainer = new DecisionTreeRegressionTrainer(10, 0);

            // Train decision tree model.
            DecisionTreeNode mdl = trainer.fit(
                ignite,
                trainingSet,
                (k, v) -> VectorUtils.of(v.x),
                (k, v) -> v.y
            );

            System.out.println(">>> Decision tree regression model: " + mdl);

            System.out.println(">>> ---------------------------------");
            System.out.println(">>> | Prediction\t| Ground Truth\t|");
            System.out.println(">>> ---------------------------------");

            // Calculate score.
            for (int x = 0; x < 10; x++) {
                double predicted = mdl.apply(VectorUtils.of(x));

                System.out.printf(">>> | %.4f\t\t| %.4f\t\t|\n", predicted, Math.sin(x));
            }

            System.out.println(">>> ---------------------------------");

            System.out.println(">>> Decision tree regression trainer example completed.");
        }
    }

    /**
     * Generates {@code sin(x)} on interval {@code [0, 10)} and loads into the specified cache.
     */
    private static void generatePoints(IgniteCache<Integer, Point> trainingSet) {
        for (int i = 0; i < 1000; i++) {
            double x = i / 100.0;
            double y = Math.sin(x);

            trainingSet.put(i, new Point(x, y));
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
}
