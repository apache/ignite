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
import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.preprocessing.encoding.stringencoder.StringEncoderTrainer;
import org.apache.ignite.ml.preprocessing.imputing.ImputerTrainer;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;
import org.apache.ignite.ml.selection.cv.CrossValidation;
import org.apache.ignite.ml.selection.scoring.evaluator.Evaluator;
import org.apache.ignite.ml.selection.scoring.metric.Accuracy;
import org.apache.ignite.ml.selection.split.TrainTestDatasetSplitter;
import org.apache.ignite.ml.selection.split.TrainTestSplit;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.ignite.thread.IgniteThread;

/**
 * To choose the best hyperparameters the cross-validation will be used in this example.
 *
 * The purpose of cross-validation is model checking, not model building.
 *
 * We train k different models.
 *
 * They differ in that 1/(k-1)th of the training data is exchanged against other cases.
 *
 * These models are sometimes called surrogate models because the (average) performance measured for these models
 * is taken as a surrogate of the performance of the model trained on all cases.
 *
 * All scenarios are described there: https://sebastianraschka.com/faq/docs/evaluate-a-model.html
 */
public class Step_8_CV {
    /** Run example. */
    public static void main(String[] args) throws InterruptedException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                Step_8_CV.class.getSimpleName(), () -> {
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

                    // Tune hyperparams with K-fold Cross-Validation on the splitted training set.
                    int[] pSet = new int[]{1, 2};
                    int[] maxDeepSet = new int[]{1, 2, 3, 4, 5, 10, 20};
                    int bestP = 1;
                    int bestMaxDeep = 1;
                    double avg = Double.MIN_VALUE;

                    for(int p: pSet){
                        for(int maxDeep: maxDeepSet){
                            IgniteBiFunction<Integer, Object[], Vector> normalizationPreprocessor = new NormalizationTrainer<Integer, Object[]>()
                                .withP(p)
                                .fit(
                                    ignite,
                                    dataCache,
                                    minMaxScalerPreprocessor
                                );

                            DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(maxDeep, 0);

                            CrossValidation<DecisionTreeNode, Double, Integer, Object[]> scoreCalculator
                                = new CrossValidation<>();

                            double[] scores = scoreCalculator.score(
                                trainer,
                                new Accuracy<>(),
                                ignite,
                                dataCache,
                                split.getTrainFilter(),
                                normalizationPreprocessor,
                                lbExtractor,
                                3
                            );

                            System.out.println("Scores are: " + Arrays.toString(scores));

                            final double currAvg = Arrays.stream(scores).average().orElse(Double.MIN_VALUE);

                            if(currAvg > avg) {
                                avg = currAvg;
                                bestP = p;
                                bestMaxDeep = maxDeep;
                            }

                            System.out.println("Avg is: " + currAvg + " with p: " + p + " with maxDeep: " + maxDeep);
                        }
                    }

                    System.out.println("Train with p: " + bestP + " and maxDeep: " + bestMaxDeep);

                    IgniteBiFunction<Integer, Object[], Vector> normalizationPreprocessor = new NormalizationTrainer<Integer, Object[]>()
                        .withP(bestP)
                        .fit(
                            ignite,
                            dataCache,
                            minMaxScalerPreprocessor
                        );

                    DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(bestMaxDeep, 0);

                    // Train decision tree model.
                    DecisionTreeNode bestMdl = trainer.fit(
                        ignite,
                        dataCache,
                        split.getTrainFilter(),
                        normalizationPreprocessor,
                        lbExtractor
                    );

                    double accuracy = Evaluator.evaluate(
                        dataCache,
                        split.getTestFilter(),
                        bestMdl,
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
