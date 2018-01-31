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

package org.apache.ignite.examples.ml.knn.regression;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.knn.models.KNNStrategy;
import org.apache.ignite.ml.knn.regression.KNNMultipleLinearRegression;
import org.apache.ignite.ml.math.distances.ManhattanDistance;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledDatasetTestTrainPair;
import org.apache.ignite.ml.structures.preprocessing.LabeledDatasetLoader;
import org.apache.ignite.ml.structures.preprocessing.LabellingMachine;
import org.apache.ignite.ml.structures.preprocessing.Normalizer;
import org.apache.ignite.thread.IgniteThread;

/**
 * <p>
 * Example of using {@link KNNMultipleLinearRegression} with iris dataset.</p>
 * <p>
 * Note that in this example we cannot guarantee order in which nodes return results of intermediate
 * computations and therefore algorithm can return different results.</p>
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} examples/config/example-ignite.xml'}.</p>
 * <p>
 * Alternatively you can run {@link ExampleNodeStartup} in another JVM which will start node
 * with {@code examples/config/example-ignite.xml} configuration.</p>
 */
public class KNNRegressionExample {
    /** Separator. */
    private static final String SEPARATOR = ",";

    /** */
    private static final String KNN_CLEARED_MACHINES_TXT = "examples/src/main/resources/datasets/cleared_machines.txt";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws InterruptedException {
        System.out.println(">>> kNN regression example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                KNNRegressionExample.class.getSimpleName(), () -> {

                try {
                    // Prepare path to read
                    File file = IgniteUtils.resolveIgnitePath(KNN_CLEARED_MACHINES_TXT);
                    if (file == null)
                        throw new RuntimeException("Can't find file: " + KNN_CLEARED_MACHINES_TXT);

                    Path path = file.toPath();

                    // Read dataset from file
                    LabeledDataset dataset = LabeledDatasetLoader.loadFromTxtFile(path, SEPARATOR, false, false);

                    // Normalize dataset
                    Normalizer.normalizeWithMiniMax(dataset);

                    // Random splitting of iris data as 80% train and 20% test datasets
                    LabeledDatasetTestTrainPair split = new LabeledDatasetTestTrainPair(dataset, 0.2);

                    System.out.println("\n>>> Amount of observations in train dataset: " + split.train().rowSize());
                    System.out.println("\n>>> Amount of observations in test dataset: " + split.test().rowSize());

                    LabeledDataset test = split.test();
                    LabeledDataset train = split.train();

                    // Builds weighted kNN-regression with Manhattan Distance
                    KNNMultipleLinearRegression knnMdl = new KNNMultipleLinearRegression(7, new ManhattanDistance(),
                        KNNStrategy.WEIGHTED, train);

                    // Clone labels
                    final double[] labels = test.labels();

                    // Save predicted classes to test dataset
                    LabellingMachine.assignLabels(test, knnMdl);

                    // Calculate mean squared error (MSE)
                    double mse = 0.0;

                    for (int i = 0; i < test.rowSize(); i++)
                        mse += Math.pow(test.label(i) - labels[i], 2.0);
                    mse = mse / test.rowSize();

                    System.out.println("\n>>> Mean squared error (MSE) " + mse);

                    // Calculate mean absolute error (MAE)
                    double mae = 0.0;

                    for (int i = 0; i < test.rowSize(); i++)
                        mae += Math.abs(test.label(i) - labels[i]);
                    mae = mae / test.rowSize();

                    System.out.println("\n>>> Mean absolute error (MAE) " + mae);

                    // Calculate R^2 as 1 - RSS/TSS
                    double avg = 0.0;

                    for (int i = 0; i < test.rowSize(); i++)
                        avg += test.label(i);

                    avg = avg / test.rowSize();

                    double detCf = 0.0;
                    double tss = 0.0;

                    for (int i = 0; i < test.rowSize(); i++) {
                        detCf += Math.pow(test.label(i) - labels[i], 2.0);
                        tss += Math.pow(test.label(i) - avg, 2.0);
                    }

                    detCf = 1 - detCf / tss;

                    System.out.println("\n>>> R^2 " + detCf);
                }
                catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("\n>>> Unexpected exception, check resources: " + e);
                }
                finally {
                    System.out.println("\n>>> kNN regression example completed.");
                }
            });

            igniteThread.start();
            igniteThread.join();
        }
    }
}
