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

package org.apache.ignite.examples.ml.inference.exchange;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ml.util.MLSandboxDatasets;
import org.apache.ignite.examples.ml.util.SandboxMLCache;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesTrainer;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.MetricName;

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
public class DiscreteNaiveBayesExportImportExample {
    /**
     * Run example.
     */
    public static void main(String[] args) throws IOException {
        System.out.println(">>> Discrete naive Bayes classification model over partitioned dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = null;
            Path jsonMdlPath = null;
            try {
                dataCache = new SandboxMLCache(ignite).fillCacheWith(MLSandboxDatasets.ENGLISH_VS_SCOTTISH);

                double[][] thresholds = new double[][] {{.5}, {.5}, {.5}, {.5}, {.5}};
                System.out.println(">>> Create new Discrete naive Bayes classification trainer object.");
                DiscreteNaiveBayesTrainer trainer = new DiscreteNaiveBayesTrainer()
                    .setBucketThresholds(thresholds);

                System.out.println("\n>>> Perform the training to get the model.");
                Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>()
                    .labeled(Vectorizer.LabelCoordinate.FIRST);

                DiscreteNaiveBayesModel mdl = trainer.fit(ignite, dataCache, vectorizer);
                System.out.println("\n>>> Exported Discrete Naive Bayes model: " + mdl.toString(true));

                double accuracy = Evaluator.evaluate(
                    dataCache,
                    mdl,
                    vectorizer,
                    MetricName.ACCURACY
                );

                System.out.println("\n>>> Accuracy for exported Discrete Naive Bayes model:" + accuracy);

                jsonMdlPath = Files.createTempFile(null, null);
                mdl.toJSON(jsonMdlPath);

                DiscreteNaiveBayesModel modelImportedFromJSON = DiscreteNaiveBayesModel.fromJSON(jsonMdlPath);

                System.out.println("\n>>> Imported Discrete Naive Bayes model: " + modelImportedFromJSON.toString(true));

                accuracy = Evaluator.evaluate(
                    dataCache,
                    modelImportedFromJSON,
                    vectorizer,
                    MetricName.ACCURACY
                );

                System.out.println("\n>>> Accuracy for imported Discrete Naive Bayes model:" + accuracy);

                System.out.println("\n>>> Discrete Naive bayes model over partitioned dataset usage example completed.");
            }
            finally {
                if (dataCache != null)
                    dataCache.destroy();
                if (jsonMdlPath != null)
                    Files.deleteIfExists(jsonMdlPath);
            }
        }
        finally {
            System.out.flush();
        }
    }

}
