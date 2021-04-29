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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ml.util.MLSandboxDatasets;
import org.apache.ignite.examples.ml.util.SandboxMLCache;
import org.apache.ignite.ml.composition.ModelsComposition;
import org.apache.ignite.ml.composition.boosting.GDBTrainer;
import org.apache.ignite.ml.composition.boosting.convergence.median.MedianOfMedianConvergenceCheckerFactory;
import org.apache.ignite.ml.dataset.feature.extractor.Vectorizer;
import org.apache.ignite.ml.dataset.feature.extractor.impl.ObjectArrayVectorizer;
import org.apache.ignite.ml.preprocessing.Preprocessor;
import org.apache.ignite.ml.preprocessing.encoding.EncoderTrainer;
import org.apache.ignite.ml.preprocessing.encoding.EncoderType;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.classification.Accuracy;
import org.apache.ignite.ml.tree.boosting.GDBBinaryClassifierOnTreesTrainer;

/**
 * Example that shows how to use Target Encoder preprocessor to encode labels presented as a mean target value.
 * <p>
 * Code in this example launches Ignite grid and fills the cache with test data (based on mushrooms dataset).</p>
 * <p>
 * After that it defines preprocessors that extract features from an upstream data and encode category with avarage
 * target value (categories). </p>
 * <p>
 * Then, it trains the model based on the processed data using gradient boosing decision tree classification.</p>
 * <p>
 * Finally, this example uses {@link Evaluator} functionality to compute metrics from predictions.</p>
 *
 * <p>Daniele Miccii-Barreca (2001). A Preprocessing Scheme for High-Cardinality Categorical
 * Attributes in Classification and Prediction Problems. SIGKDD Explor. Newsl. 3, 1.
 * From http://dx.doi.org/10.1145/507533.507538</p>
 */
public class TargetEncoderExample {
    /**
     * Run example.
     */
    public static void main(String[] args) {
        System.out.println();
        System.out.println(">>> Train Gradient Boosing Decision Tree model on amazon-employee-access-challenge_train.csv dataset.");

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            try {
                IgniteCache<Integer, Object[]> dataCache = new SandboxMLCache(ignite)
                    .fillObjectCacheWithCategoricalData(MLSandboxDatasets.AMAZON_EMPLOYEE_ACCESS);

                Set<Integer> featuresIndexies = new HashSet<>(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9));
                Set<Integer> targetEncodedfeaturesIndexies = new HashSet<>(Arrays.asList(1, 5, 6));
                Integer targetIndex = 0;

                final Vectorizer<Integer, Object[], Integer, Object> vectorizer = new ObjectArrayVectorizer<Integer>(featuresIndexies.toArray(new Integer[0]))
                    .labeled(targetIndex);

                Preprocessor<Integer, Object[]> strEncoderPreprocessor = new EncoderTrainer<Integer, Object[]>()
                    .withEncoderType(EncoderType.STRING_ENCODER)
                    .withEncodedFeature(0)
                    .withEncodedFeatures(featuresIndexies)
                    .fit(ignite,
                        dataCache,
                        vectorizer
                    );

                Preprocessor<Integer, Object[]> targetEncoderProcessor = new EncoderTrainer<Integer, Object[]>()
                    .withEncoderType(EncoderType.TARGET_ENCODER)
                    .labeled(0)
                    .withEncodedFeatures(targetEncodedfeaturesIndexies)
                    .minSamplesLeaf(1)
                    .minCategorySize(1L)
                    .smoothing(1d)
                    .fit(ignite,
                        dataCache,
                        strEncoderPreprocessor
                    );

                Preprocessor<Integer, Object[]> lbEncoderPreprocessor = new EncoderTrainer<Integer, Object[]>()
                    .withEncoderType(EncoderType.LABEL_ENCODER)
                    .fit(ignite,
                        dataCache,
                        targetEncoderProcessor
                    );

                GDBTrainer trainer = new GDBBinaryClassifierOnTreesTrainer(0.5, 500, 4, 0.)
                    .withCheckConvergenceStgyFactory(new MedianOfMedianConvergenceCheckerFactory(0.1));

                // Train model.
                ModelsComposition mdl = trainer.fit(
                    ignite,
                    dataCache,
                    lbEncoderPreprocessor
                );

                System.out.println("\n>>> Trained model: " + mdl);

                double accuracy = Evaluator.evaluate(
                    dataCache,
                    mdl,
                    lbEncoderPreprocessor,
                    new Accuracy()
                );

                System.out.println("\n>>> Accuracy " + accuracy);
                System.out.println("\n>>> Test Error " + (1 - accuracy));

                System.out.println(">>> Train Gradient Boosing Decision Tree model on amazon-employee-access-challenge_train.csv dataset.");

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
