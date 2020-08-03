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

package org.apache.ignite.examples.ml.selection.cv;

import java.util.Arrays;
import java.util.Random;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.ml.dataset.feature.extractor.impl.LabeledDummyVectorizer;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.apache.ignite.ml.selection.cv.CrossValidation;
import org.apache.ignite.ml.selection.scoring.metric.MetricName;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;

/**
 * Run <a href="https://en.wikipedia.org/wiki/Decision_tree">decision tree</a> classification with
 * <a href="https://en.wikipedia.org/wiki/Cross-validation_(statistics)">cross validation</a> ({@link
 * CrossValidation}).
 * <p>
 * Code in this example launches Ignite grid and fills the cache with pseudo random training data points.</p>
 * <p>
 * After that it creates classification trainer ({@link DecisionTreeClassificationTrainer}) and computes cross-validated
 * metrics based on the training set.</p>
 */
public class CrossValidationExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String... args) {
        System.out.println(">>> Cross validation score calculator example started.");

        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            // Create cache with training data.
            CacheConfiguration<Integer, LabeledVector<Double>> trainingSetCfg = new CacheConfiguration<>();
            trainingSetCfg.setName("TRAINING_SET");
            trainingSetCfg.setAffinity(new RendezvousAffinityFunction(false, 10));

            IgniteCache<Integer, LabeledVector<Double>> trainingSet = null;
            try {
                trainingSet = ignite.createCache(trainingSetCfg);

                Random rnd = new Random(0);

                // Fill training data.
                for (int i = 0; i < 1000; i++)
                    trainingSet.put(i, generatePoint(rnd));

                // Create classification trainer.
                DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(4, 0);

                LabeledDummyVectorizer<Integer, Double> vectorizer = new LabeledDummyVectorizer<>();

                CrossValidation<DecisionTreeNode, Integer, LabeledVector<Double>> scoreCalculator
                    = new CrossValidation<>();

                double[] accuracyScores = scoreCalculator
                    .withIgnite(ignite)
                    .withUpstreamCache(trainingSet)
                    .withTrainer(trainer)
                    .withMetric(MetricName.ACCURACY)
                    .withPreprocessor(vectorizer)
                    .withAmountOfFolds(4)
                    .isRunningOnPipeline(false)
                    .scoreByFolds();

                System.out.println(">>> Accuracy: " + Arrays.toString(accuracyScores));

                double[] balancedAccuracyScores = scoreCalculator
                    .withMetric(MetricName.ACCURACY)
                    .scoreByFolds();

                System.out.println(">>> Balanced Accuracy: " + Arrays.toString(balancedAccuracyScores));

                System.out.println(">>> Cross validation score calculator example completed.");
            }
            finally {
                trainingSet.destroy();
            }
        }
        finally {
            System.out.flush();
        }
    }

    /**
     * Generate point with {@code x} in (-0.5, 0.5) and {@code y} in the same interval. If {@code x * y > 0} then label
     * is 1, otherwise 0.
     *
     * @param rnd Random.
     * @return Point with label.
     */
    private static LabeledVector<Double> generatePoint(Random rnd) {

        double x = rnd.nextDouble() - 0.5;
        double y = rnd.nextDouble() - 0.5;

        return new LabeledVector<>(
            VectorUtils.of(x, y),
            x * y > 0 ? 1.0 : 0.0
        );
    }
}
