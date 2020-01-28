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

package org.apache.ignite.examples.ml.tree.randomforest;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.cache.Cache;
import org.apache.commons.math3.util.Precision;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.examples.ml.util.MLSandboxDatasets;
import org.apache.ignite.examples.ml.util.SandboxMLCache;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.dataset.feature.FeatureMeta;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.DummyVectorizer;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.tree.randomforest.RandomForestClassifierTrainer;
import org.apache.ignite.ml.tree.randomforest.data.FeaturesCountSelectionStrategies;

/**
 * Example represents a solution for the task of wine classification based on a
 * <a href ="https://en.wikipedia.org/wiki/Random_forest">Random Forest</a> implementation for
 * multi-classification.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with test data points (based on the
 * <a href="https://archive.ics.uci.edu/ml/machine-learning-databases/wine/">Wine recognition dataset</a>).</p>
 * <p>
 * After that it initializes the  {@link RandomForestClassifierTrainer} with thread pool for multi-thread learning and
 * trains the model based on the specified data using random forest regression algorithm.</p>
 * <p>
 * Finally, this example loops over the test set of data points, compares prediction of the trained model to the
 * expected outcome (ground truth), and evaluates accuracy of the model.</p>
 * <p>
 * You can change the test data used in this example and re-run it to explore this algorithm further.</p>
 */
public class RandomForestClassificationExample {
    /**
     * Run example.
     */
    public static void main(String[] args) throws IOException {
        System.out.println();
        System.out.println(">>> Random Forest multi-class classification algorithm over cached dataset usage example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteCache<Integer, Vector> dataCache = null;
            try {
                dataCache = new SandboxMLCache(ignite).fillCacheWith(MLSandboxDatasets.WINE_RECOGNITION);

                AtomicInteger idx = new AtomicInteger(0);
                RandomForestClassifierTrainer classifier = new RandomForestClassifierTrainer(
                    IntStream.range(0, dataCache.get(1).size() - 1).mapToObj(
                        x -> new FeatureMeta("", idx.getAndIncrement(), false)).collect(Collectors.toList())
                ).withAmountOfTrees(101)
                    .withFeaturesCountSelectionStrgy(FeaturesCountSelectionStrategies.ONE_THIRD)
                    .withMaxDepth(4)
                    .withMinImpurityDelta(0.)
                    .withSubSampleSize(0.3)
                    .withSeed(0);

                System.out.println(">>> Configured trainer: " + classifier.getClass().getSimpleName());

                Vectorizer<Integer, Vector, Integer, Double> vectorizer = new DummyVectorizer<Integer>()
                    .labeled(Vectorizer.LabelCoordinate.FIRST);
                ModelsComposition randomForestMdl = classifier.fit(ignite, dataCache, vectorizer);

                System.out.println(">>> Trained model: " + randomForestMdl.toString(true));

                int amountOfErrors = 0;
                int totalAmount = 0;

                try (QueryCursor<Cache.Entry<Integer, Vector>> observations = dataCache.query(new ScanQuery<>())) {
                    for (Cache.Entry<Integer, Vector> observation : observations) {
                        Vector val = observation.getValue();
                        Vector inputs = val.copyOfRange(1, val.size());
                        double groundTruth = val.get(0);

                        double prediction = randomForestMdl.predict(inputs);

                        totalAmount++;
                        if (!Precision.equals(groundTruth, prediction, Precision.EPSILON))
                            amountOfErrors++;
                    }

                    System.out.println("\n>>> Evaluated model on " + totalAmount + " data points.");

                    System.out.println("\n>>> Absolute amount of errors " + amountOfErrors);
                    System.out.println("\n>>> Accuracy " + (1 - amountOfErrors / (double)totalAmount));
                    System.out.println(">>> Random Forest multi-class classification algorithm over cached dataset usage example completed.");
                }

            }
            finally {
                if (dataCache != null)
                    dataCache.destroy();
            }
        }
        finally {
            System.out.flush();
        }
    }
}
