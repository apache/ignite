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
import org.apache.ignite.examples.ml.tutorial.Step_1_Read_and_Learn;
import org.apache.ignite.examples.ml.util.DatasetHelper;
import org.apache.ignite.ml.dataset.DatasetFactory;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.ObjectArrayVectorizer;
import org.apache.ignite.ml.dataset.primitive.SimpleDataset;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.encoding.EncoderTrainer;
import org.apache.ignite.ml.preprocessing.encoding.EncoderType;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.classification.Accuracy;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.ignite.ml.util.MLSandboxDatasets;
import org.apache.ignite.ml.util.SandboxMLCache;

/**
 * Let's add two categorial features "sex", "embarked" to predict more precisely than in {@link
 * Step_1_Read_and_Learn}..
 * <p>
 * To encode categorial features the {@link EncoderTrainer} of the
 * <a href="https://en.wikipedia.org/wiki/One-hot">One-hot</a> type will be used.</p>
 * <p>
 * Code in this example launches Ignite grid and fills the cache with test data (based on Titanic passengers data).</p>
 * <p>
 * After that it defines preprocessors that extract features from an upstream data and encode string values (categories)
 * to double values in specified range.</p>
 * <p>
 * Then, it trains the model based on the processed data using decision tree classification.</p>
 * <p>
 * Finally, this example uses {@link Evaluator} functionality to compute metrics from predictions.</p>
 */
public class BrokenEncoderExampleWithStringLabels {
    /**
     * Run example.
     */
    public static void main(String[] args) throws Exception {
        System.out.println();
        System.out.println(">>> Train Decision Tree model on mushrooms.csv dataset.");

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            try {
                IgniteCache<Integer, Object[]> dataCache = new SandboxMLCache(ignite)
                    .fillObjectCacheWith2(MLSandboxDatasets.MUSHROOMS);

                final Vectorizer<Integer, Object[], Integer, Object> vectorizer = new ObjectArrayVectorizer<Integer>(1, 2).labeled(0);

                Preprocessor<Integer, Object[]> encoderPreprocessor = new EncoderTrainer<Integer, Object[]>()
                    .withEncoderType(EncoderType.STRING_ENCODER)
                    .withEncodedFeature(0)
                    .withEncodedFeature(1)
                    .fit(ignite,
                        dataCache,
                        vectorizer
                    );

                try (SimpleDataset<?> dataset = DatasetFactory.createSimpleDataset(ignite, dataCache, encoderPreprocessor)) {
                    new DatasetHelper(dataset).describe();
                }

                DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(5, 0);

                // Train decision tree model.
                DecisionTreeNode mdl = trainer.fit(
                    ignite,
                    dataCache,
                    encoderPreprocessor
                );

                System.out.println("\n>>> Trained model: " + mdl);

                double accuracy = Evaluator.evaluate(
                    dataCache,
                    mdl,
                    encoderPreprocessor,
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
