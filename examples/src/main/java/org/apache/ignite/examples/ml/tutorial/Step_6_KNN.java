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
import javax.cache.Cache;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.ml.knn.classification.KNNClassificationModel;
import org.apache.ignite.ml.knn.classification.KNNClassificationTrainer;
import org.apache.ignite.ml.knn.classification.KNNStrategy;
import org.apache.ignite.ml.math.functions.IgniteBiFunction;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.preprocessing.encoding.stringencoder.StringEncoderTrainer;
import org.apache.ignite.ml.preprocessing.imputing.ImputerTrainer;
import org.apache.ignite.ml.preprocessing.minmaxscaling.MinMaxScalerTrainer;
import org.apache.ignite.ml.preprocessing.normalization.NormalizationTrainer;
import org.apache.ignite.thread.IgniteThread;

/**
 * Sometimes is better to change algorithm, let's say on kNN.
 */
public class Step_6_KNN {
    /** Run example. */
    public static void main(String[] args) throws InterruptedException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                Step_6_KNN.class.getSimpleName(), () -> {
                try {
                    IgniteCache<Integer, Object[]> dataCache = TitanicUtils.readPassengers(ignite);

                    // Defines first preprocessor that extracts features from an upstream data.
                    // Extracts "pclass", "sibsp", "parch", "sex", "embarked", "age", "fare"
                    IgniteBiFunction<Integer, Object[], Object[]> featureExtractor
                        = (k, v) -> new Object[]{v[0], v[3], v[4], v[5], v[6], v[8], v[10]};


                    IgniteBiFunction<Integer, Object[], double[]> strEncoderPreprocessor = new StringEncoderTrainer<Integer, Object[]>()
                        .encodeFeature(1)
                        .encodeFeature(6) // <--- Changed index here
                        .fit(ignite,
                            dataCache,
                            featureExtractor
                    );

                    IgniteBiFunction<Integer, Object[], double[]> imputingPreprocessor = new ImputerTrainer<Integer, Object[]>()
                        .fit(ignite,
                            dataCache,
                            strEncoderPreprocessor
                        );


                    IgniteBiFunction<Integer, Object[], double[]> minMaxScalerPreprocessor = new MinMaxScalerTrainer<Integer, Object[]>()
                        .fit(
                        ignite,
                        dataCache,
                        imputingPreprocessor
                    );

                    IgniteBiFunction<Integer, Object[], double[]> normalizationPreprocessor = new NormalizationTrainer<Integer, Object[]>()
                        .withP(1)
                        .fit(
                        ignite,
                        dataCache,
                        minMaxScalerPreprocessor
                    );

                    KNNClassificationTrainer trainer = new KNNClassificationTrainer();

                    // Train decision tree model.
                    KNNClassificationModel mdl = trainer.fit(
                        ignite,
                        dataCache,
                        normalizationPreprocessor,
                        (k, v) -> (double)v[1]
                    ).withK(1).withStrategy(KNNStrategy.WEIGHTED);


                    System.out.println(">>> ----------------------------------------------------------------");
                    System.out.println(">>> | Prediction\t| Ground Truth\t| Name\t|");
                    System.out.println(">>> ----------------------------------------------------------------");

                    int amountOfErrors = 0;
                    int totalAmount = 0;

                    // Build confusion matrix. See https://en.wikipedia.org/wiki/Confusion_matrix
                    int[][] confusionMtx = {{0, 0}, {0, 0}};

                    try (QueryCursor<Cache.Entry<Integer, Object[]>> observations = dataCache.query(new ScanQuery<>())) {
                        for (Cache.Entry<Integer, Object[]> observation : observations) {

                            Object[] val = observation.getValue();
                            double groundTruth = (double)val[1];
                            String name = (String)val[2];

                            double prediction = mdl.apply(new DenseLocalOnHeapVector(normalizationPreprocessor.apply(observation.getKey(), val)));

                            totalAmount++;
                            if (groundTruth != prediction)
                                amountOfErrors++;

                            int idx1 = (int)prediction;
                            int idx2 = (int)groundTruth;

                            confusionMtx[idx1][idx2]++;

                            System.out.printf(">>>| %.4f\t\t| %.4f\t\t\t\t\t\t| %s\t\t\t\t\t\t\t\t\t\t|\n", prediction, groundTruth, name);
                        }

                        System.out.println(">>> ---------------------------------");

                        System.out.println("\n>>> Absolute amount of errors " + amountOfErrors);
                        double accuracy = 1 - amountOfErrors / (double)totalAmount;
                        System.out.println("\n>>> Accuracy " + accuracy);
                        System.out.println("\n>>> Test Error " + (1 - accuracy));

                        System.out.println("\n>>> Confusion matrix is " + Arrays.deepToString(confusionMtx));
                        System.out.println(">>> ---------------------------------");
                    }
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
