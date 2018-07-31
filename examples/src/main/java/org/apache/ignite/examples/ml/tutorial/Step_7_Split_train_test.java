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

import java.io.FileNotFoundException;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.encoding.stringencoder.StringEncoderTrainer;
import org.apache.ignite.ml.preprocessing.imputing.ImputerTrainer;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.Accuracy;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.ignite.thread.IgniteThread;

/**
 * The highest accuracy in the previous example is the result of overfitting.
 *
 * For real model estimation is better to use test-train split via TrainTestDatasetSplitter.
 */
public class Step_7_Split_train_test {
    /** Run example. */
    public static void main(String[] args) throws InterruptedException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                Step_7_Split_train_test.class.getSimpleName(), () -> {
                try {
                    IgniteCache<Integer, Object[]> dataCache = TitanicUtils.readPassengers(ignite);

                    // Defines first preprocessor that extracts features from an upstream data.
                    // Extracts "pclass", "sibsp", "parch", "sex", "embarked", "age", "fare"
                    IgniteBiFunction<Integer, Object[], Object[]> featureExtractor
                        = (k, v) -> new Object[]{v[0], v[3], v[4], v[5], v[6], v[8], v[10]};

                    IgniteBiFunction<Integer, Object[], Double> lbExtractor = (k, v) -> (double) v[1];

                    TrainTestSplit<Integer, Object[]> split = new TrainTestDatasetSplitter<Integer, Object[]>()
                        .split(0.75);

                    IgniteBiFunction<Integer, Object[], Vector> strEncoderPreprocessor = new StringEncoderTrainer<Integer, Object[]>()
                        .encodeFeature(1)
                        .encodeFeature(6) // <--- Changed index here
                        .fit(ignite,
                            dataCache,
                            featureExtractor
                    );

                    IgniteBiFunction<Integer, Object[], Vector> imputingPreprocessor = new ImputerTrainer<Integer, Object[]>()
                        .fit(ignite,
                            dataCache,
                            strEncoderPreprocessor
                        );


                    IgniteBiFunction<Integer, Object[], Vector> minMaxScalerPreprocessor = new MinMaxScalerTrainer<Integer, Object[]>()
                        .fit(
                        ignite,
                        dataCache,
                        imputingPreprocessor
                    );

                    IgniteBiFunction<Integer, Object[], Vector> normalizationPreprocessor = new NormalizationTrainer<Integer, Object[]>()
                        .withP(1)
                        .fit(
                        ignite,
                        dataCache,
                        minMaxScalerPreprocessor
                    );

                    DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(5, 0);

                    // Train decision tree model.
                    DecisionTreeNode mdl = trainer.fit(
                        ignite,
                        dataCache,
                        split.getTrainFilter(),
                        normalizationPreprocessor,
                        lbExtractor
                    );

                    double accuracy = Evaluator.evaluate(
                        dataCache,
                        split.getTestFilter(),
                        mdl,
                        normalizationPreprocessor,
                        lbExtractor,
                        new Accuracy<>()
                    );

                    System.out.println("\n>>> Accuracy " + accuracy);
                    System.out.println("\n>>> Test Error " + (1 - accuracy));
                }
                catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            });

            igniteThread.start();

            igniteThread.join();
        }
    }
}
