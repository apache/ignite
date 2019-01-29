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

package org.apache.ignite.examples.ml.naivebayes;

import java.io.FileNotFoundException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesTrainer;
import org.apache.ignite.ml.selection.scoring.evaluator.BinaryClassificationEvaluator;
import org.apache.ignite.ml.util.MLSandboxDatasets;
import org.apache.ignite.ml.util.SandboxMLCache;

/**
 * Run naive Bayes classification model based on <a href=https://en.wikipedia.org/wiki/Naive_Bayes_classifier#Multinomial_naive_Bayes">
 * naive Bayes classifier</a> algorithm ({@link DiscreteNaiveBayesTrainer}) over distributed cache.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with test data points.
 * </p>
 * <p>
 * After that it trains the Discrete naive Bayes classification model based on the specified data.</p>
 * <p>
 * Finally, this example loops over the test set of data points, applies the trained model to predict the target value,
 * compares prediction to expected outcome (ground truth), and builds
 * <a href="https://en.wikipedia.org/wiki/Confusion_matrix">confusion matrix</a>.</p>
 * <p>
 * You can change the test data used in this example and re-run it to explore this algorithm further.</p>
 */
public class DiscreteNaiveBayesTrainerExample {
    /** Run example. */
    public static void main(String[] args) throws FileNotFoundException {
        System.out.println();
        System.out.println(">>> Discrete naive Bayes classification model over partitioned dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = new SandboxMLCache(ignite)
                .fillCacheWith(MLSandboxDatasets.ENGLISH_VS_SCOTTISH);

            double[][] thresholds = new double[][] {{.5}, {.5}, {.5}, {.5}, {.5}};
            System.out.println(">>> Create new Discrete naive Bayes classification trainer object.");
            DiscreteNaiveBayesTrainer trainer = new DiscreteNaiveBayesTrainer()
                .setBucketThresholds(thresholds);

            System.out.println(">>> Perform the training to get the model.");
            IgniteBiFunction<Integer, Vector, Vector> featureExtractor = (k, v) -> v.copyOfRange(1, v.size());
            IgniteBiFunction<Integer, Vector, Double> lbExtractor = (k, v) -> v.get(0);

            DiscreteNaiveBayesModel mdl = trainer.fit(
                ignite,
                dataCache,
                featureExtractor,
                lbExtractor
            );

            System.out.println(">>> Discrete Naive Bayes model: " + mdl);

            double accuracy = BinaryClassificationEvaluator.evaluate(
                dataCache,
                mdl,
                featureExtractor,
                lbExtractor
            ).accuracy();

            System.out.println("\n>>> Accuracy " + accuracy);

            System.out.println(">>> Discrete Naive bayes model over partitioned dataset usage example completed.");
        }
    }

}
