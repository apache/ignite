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
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.ignite.thread.IgniteThread;

import java.util.Random;

/**
 * Example of using distributed {@link DecisionTreeClassificationTrainer}.
 */
public class DecisionTreeClassificationTrainerExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String... args) throws InterruptedException {
        System.out.println(">>> Decision tree classification trainer example started.");

        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                DecisionTreeClassificationTrainerExample.class.getSimpleName(), () -> {

                // Create cache with training data.
                CacheConfiguration<Integer, LabeledPoint> trainingSetCfg = new CacheConfiguration<>();
                trainingSetCfg.setName("TRAINING_SET");
                trainingSetCfg.setAffinity(new RendezvousAffinityFunction(false, 10));

                IgniteCache<Integer, LabeledPoint> trainingSet = ignite.createCache(trainingSetCfg);

                Random rnd = new Random(0);

                // Fill training data.
                for (int i = 0; i < 1000; i++)
                    trainingSet.put(i, generatePoint(rnd));

                // Create classification trainer.
                DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(4, 0);

                // Train decision tree model.
                DecisionTreeNode mdl = trainer.fit(
                    ignite,
                    trainingSet,
                    (k, v) -> new double[]{v.x, v.y},
                    (k, v) -> v.lb
                );

                // Calculate score.
                int correctPredictions = 0;
                for (int i = 0; i < 1000; i++) {
                    LabeledPoint pnt = generatePoint(rnd);

                    double prediction = mdl.apply(new double[]{pnt.x, pnt.y});

                    if (prediction == pnt.lb)
                        correctPredictions++;
                }

                System.out.println(">>> Accuracy: " + correctPredictions / 10.0 + "%");

                System.out.println(">>> Decision tree classification trainer example completed.");
            });

            igniteThread.start();

            igniteThread.join();
        }
    }

    /**
     * Generate point with {@code x} in (-0.5, 0.5) and {@code y} in the same interval. If {@code x * y > 0} then label
     * is 1, otherwise 0.
     *
     * @param rnd Random.
     * @return Point with label.
     */
    private static LabeledPoint generatePoint(Random rnd) {

        double x = rnd.nextDouble() - 0.5;
        double y = rnd.nextDouble() - 0.5;

        return new LabeledPoint(x, y, x * y > 0 ? 1 : 0);
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
