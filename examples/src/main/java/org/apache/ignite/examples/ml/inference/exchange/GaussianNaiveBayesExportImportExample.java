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
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesTrainer;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.MetricName;

/**
 * Run naive Bayes classification model based on <a href="https://en.wikipedia.org/wiki/Naive_Bayes_classifier"> naive
 * Bayes classifier</a> algorithm ({@link GaussianNaiveBayesTrainer}) over distributed cache.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with test data points (based on the
 * <a href="https://en.wikipedia.org/wiki/Iris_flower_data_set"></a>Iris dataset</a>).</p>
 * <p>
 * After that it trains the naive Bayes classification model based on the specified data.</p>
 * <p>
 * Finally, this example loops over the test set of data points, applies the trained model to predict the target value,
 * compares prediction to expected outcome (ground truth), and builds
 * <a href="https://en.wikipedia.org/wiki/Confusion_matrix">confusion matrix</a>.</p>
 * <p>
 * You can change the test data used in this example and re-run it to explore this algorithm further.</p>
 */
public class GaussianNaiveBayesExportImportExample {
    /**
     * Run example.
     */
    public static void main(String[] args) throws IOException {
        System.out.println();
        System.out.println(">>> Naive Bayes classification model over partitioned dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = null;
            Path jsonMdlPath = null;
            try {
                dataCache = new SandboxMLCache(ignite).fillCacheWith(MLSandboxDatasets.TWO_CLASSED_IRIS);

                System.out.println(">>> Create new Gaussian Naive Bayes classification trainer object.");
                GaussianNaiveBayesTrainer trainer = new GaussianNaiveBayesTrainer();

                System.out.println("\n>>> Perform the training to get the model.");

                Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>()
                    .labeled(Vectorizer.LabelCoordinate.FIRST);

                GaussianNaiveBayesModel mdl = trainer.fit(ignite, dataCache, vectorizer);
                System.out.println("\n>>> Exported Gaussian Naive Bayes model: " + mdl.toString(true));

                double accuracy = Evaluator.evaluate(
                    dataCache,
                    mdl,
                    vectorizer,
                    MetricName.ACCURACY
                );

                System.out.println("\n>>> Accuracy for exported Gaussian Naive Bayes model:" + accuracy);

                jsonMdlPath = Files.createTempFile(null, null);
                mdl.toJSON(jsonMdlPath);

                GaussianNaiveBayesModel modelImportedFromJSON = GaussianNaiveBayesModel.fromJSON(jsonMdlPath);

                System.out.println("\n>>> Imported Gaussian Naive Bayes model: " + modelImportedFromJSON.toString(true));

                accuracy = Evaluator.evaluate(
                    dataCache,
                    modelImportedFromJSON,
                    vectorizer,
                    MetricName.ACCURACY
                );

                System.out.println("\n>>> Accuracy for imported Gaussian Naive Bayes model:" + accuracy);

                System.out.println("\n>>> Gaussian Naive bayes model over partitioned dataset usage example completed.");
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
