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

package org.apache.ignite.examples.ml.preprocessing.encoding;

import java.io.FileNotFoundException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ml.util.MLSandboxDatasets;
import org.apache.ignite.examples.ml.util.SandboxMLCache;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.ObjectArrayVectorizer;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.encoding.EncoderTrainer;
import org.apache.ignite.ml.preprocessing.encoding.EncoderType;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.classification.Accuracy;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;

/**
 * Example that shows how to combine together two preprocessors: String Encoder preprocessor to encode features presented as a strings
 * and Normalizer to normalize data presented as doubles.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with test data (based on mushrooms dataset).</p>
 * <p>
 * After that it defines preprocessors that extract features from an upstream data and encode string values (categories)
 * to double values in specified range.</p>
 * <p>
 * Then, it trains the model based on the processed data using decision tree classification.</p>
 * <p>
 * Finally, this example uses {@link Evaluator} functionality to compute metrics from predictions.</p>
 */
public class EncoderExampleWithNormalization {
    /**
     * Run example.
     */
    public static void main(String[] args) {
        System.out.println();
        System.out.println(">>> Train Decision Tree model on mushrooms.csv dataset.");

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            try {
                IgniteCache<Integer, Object[]> dataCache = new SandboxMLCache(ignite)
                    .fillObjectCacheWithDoubleLabels(MLSandboxDatasets.MUSHROOMS);

                final Vectorizer<Integer, Object[], Integer, Object> vectorizer = new ObjectArrayVectorizer<Integer>(1, 2, 3).labeled(0);

                Preprocessor<Integer, Object[]> encoderPreprocessor = new EncoderTrainer<Integer, Object[]>()
                    .withEncoderType(EncoderType.STRING_ENCODER)
                    .withEncodedFeature(0)
                    .withEncodedFeature(1)
                    .withEncodedFeature(2)
                    .fit(ignite,
                        dataCache,
                        vectorizer
                    );

                // Defines second preprocessor that normalizes features.
                Preprocessor<Integer, Object[]> normalizer = new NormalizationTrainer<Integer, Object[]>()
                    .withP(1)
                    .fit(ignite, dataCache, encoderPreprocessor);

                DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(5, 0);

                // Train decision tree model.
                DecisionTreeNode mdl = trainer.fit(
                    ignite,
                    dataCache,
                    normalizer
                );

                System.out.println("\n>>> Trained model: " + mdl);

                double accuracy = Evaluator.evaluate(
                    dataCache,
                    mdl,
                    normalizer,
                    new Accuracy<>()
                );

                System.out.println("\n>>> Accuracy " + accuracy);
                System.out.println("\n>>> Test Error " + (1 - accuracy));

                System.out.println(">>> Tutorial step 3 (categorial with One-hot encoder) example started.");

            }
            catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
        finally {
            System.out.flush();
        }
    }
}
