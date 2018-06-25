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
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.apache.ignite.ml.tree.DecisionTreeClassificationTrainer;
import org.apache.ignite.ml.tree.DecisionTreeNode;
import org.apache.ignite.thread.IgniteThread;

/**
 * Usage of DecisionTreeClassificationTrainer to predict death in the disaster.
 *
 * Extract 3 features "pclass", "sibsp", "parch" to use in prediction.
 */
public class Step_1_Read_and_Learn {
    /** Run example. */
    public static void main(String[] args) throws InterruptedException {
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                Step_1_Read_and_Learn.class.getSimpleName(), () -> {
                try {
                    IgniteCache<Integer, Object[]> dataCache = TitanicUtils.readPassengers(ignite);

                    DecisionTreeClassificationTrainer trainer = new DecisionTreeClassificationTrainer(5, 0);

                    // Train decision tree model.
                    DecisionTreeNode mdl = trainer.fit(
                        ignite,
                        dataCache,
                        (k, v) -> new double[]{(double)v[0], (double)v[5], (double)v[6]}, // "pclass", "sibsp", "parch"
                        (k, v) -> (double)v[1]
                    );

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
                            double[] inputs = new double[]{(double)val[0], (double)val[5], (double)val[6]};
                            double groundTruth = (double)val[1];
                            String name = (String)val[2];

                            double prediction = mdl.apply(new DenseLocalOnHeapVector(inputs));

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
