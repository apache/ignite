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

package org.apache.ignite.examples.ml.tutorial;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.encoding.EncoderTrainer;
import org.apache.ignite.ml.preprocessing.encoding.EncoderType;
import org.apache.ignite.ml.preprocessing.imputing.ImputerTrainer;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.classification.Accuracy;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;

import java.io.FileNotFoundException;

/**
 * Let's add two categorial features "sex", "embarked" to predict more precisely than in {@link Step_1_Read_and_Learn}.
 * <p>
 * To encode categorial features the String kind type of {@link EncoderTrainer} will be used.</p>
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
public class Step_3_Categorial {
    /** Run example. */
    public static void main(String[] args) {
        System.out.println();
        System.out.println(">>> Tutorial step 3 (categorial) example started.");

        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            try {
                IgniteCache<Integer, Object[]> dataCache = TitanicUtils.readPassengers(ignite);

                // Defines first preprocessor that extracts features from an upstream data.
                IgniteBiFunction<Integer, Object[], Object[]> featureExtractor
                    = (k, v) -> new Object[]{v[0], v[3], v[5], v[6], v[10]}; // "pclass", "sibsp", "parch", "sex", "embarked"

                IgniteBiFunction<Integer, Object[], Double> lbExtractor = (k, v) -> (double) v[1];

                IgniteBiFunction<Integer, Object[], Vector> strEncoderPreprocessor = new EncoderTrainer<Integer, Object[]>()
                    .withEncoderType(EncoderType.STRING_ENCODER)
                    .withEncodedFeature(1)
                    .withEncodedFeature(4)
                    .fit(ignite,
                        dataCache,
                        featureExtractor
                );

                IgniteBiFunction<Integer, Object[], Vector> imputingPreprocessor = new ImputerTrainer<Integer, Object[]>()
                    .fit(ignite,
                        dataCache,
                        strEncoderPreprocessor
                    );

                DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(5, 0);

                // Train decision tree model.
                DecisionTreeNode mdl = trainer.fit(
                    ignite,
                    dataCache,
                    imputingPreprocessor,
                    lbExtractor
                );

                System.out.println("\n>>> Trained model: " + mdl);

                double accuracy = Evaluator.evaluate(
                    dataCache,
                    mdl,
                    imputingPreprocessor,
                    lbExtractor,
                    new Accuracy<>()
                );

                System.out.println("\n>>> Accuracy " + accuracy);
                System.out.println("\n>>> Test Error " + (1 - accuracy));

                System.out.println(">>> Tutorial step 3 (categorial) example completed.");
            }
            catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }
}
