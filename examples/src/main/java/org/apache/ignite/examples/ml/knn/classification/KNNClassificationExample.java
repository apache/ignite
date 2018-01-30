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

package org.apache.ignite.examples.ml.knn.classification;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.knn.models.KNNModel;
import org.apache.ignite.ml.knn.models.KNNStrategy;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledDatasetTestTrainPair;
import org.apache.ignite.ml.structures.preprocessing.LabeledDatasetLoader;
import org.apache.ignite.ml.structures.preprocessing.LabellingMachine;
import org.apache.ignite.thread.IgniteThread;

/**
 * <p>
 * Example of using {@link KNNModel} with iris dataset.</p>
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
public class KNNClassificationExample {
    /** Separator. */
    private static final String SEPARATOR = "\t";

    /** Path to the Iris dataset. */
    private static final String KNN_IRIS_TXT = "examples/src/main/resources/datasets/iris.txt";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws InterruptedException {
        System.out.println(">>> kNN classification example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                KNNClassificationExample.class.getSimpleName(), () -> {

                try {
                    // Prepare path to read
                    File file = IgniteUtils.resolveIgnitePath(KNN_IRIS_TXT);
                    if (file == null)
                        throw new RuntimeException("Can't find file: " + KNN_IRIS_TXT);

                    Path path = file.toPath();

                    // Read dataset from file
                    LabeledDataset dataset = LabeledDatasetLoader.loadFromTxtFile(path, SEPARATOR, true, false);

                    // Random splitting of iris data as 70% train and 30% test datasets
                    LabeledDatasetTestTrainPair split = new LabeledDatasetTestTrainPair(dataset, 0.3);

                    System.out.println("\n>>> Amount of observations in train dataset " + split.train().rowSize());
                    System.out.println("\n>>> Amount of observations in test dataset " + split.test().rowSize());

                    LabeledDataset test = split.test();
                    LabeledDataset train = split.train();

                    KNNModel knnMdl = new KNNModel(5, new EuclideanDistance(), KNNStrategy.SIMPLE, train);

                    // Clone labels
                    final double[] labels = test.labels();

                    // Save predicted classes to test dataset
                    LabellingMachine.assignLabels(test, knnMdl);

                    // Calculate amount of errors on test dataset
                    int amountOfErrors = 0;
                    for (int i = 0; i < test.rowSize(); i++) {
                        if (test.label(i) != labels[i])
                            amountOfErrors++;
                    }

                    System.out.println("\n>>> Absolute amount of errors " + amountOfErrors);
                    System.out.println("\n>>> Accuracy " + amountOfErrors / (double)test.rowSize());

                    // Build confusion matrix. See https://en.wikipedia.org/wiki/Confusion_matrix
                    int[][] confusionMtx = {{0, 0, 0}, {0, 0, 0}, {0, 0, 0}};
                    for (int i = 0; i < test.rowSize(); i++) {
                        int idx1 = (int)test.label(i);
                        int idx2 = (int)labels[i];
                        confusionMtx[idx1 - 1][idx2 - 1]++;
                    }
                    System.out.println("\n>>> Confusion matrix is " + Arrays.deepToString(confusionMtx));

                    // Calculate precision, recall and F-metric for each class
                    for (int i = 0; i < 3; i++) {
                        double precision = 0.0;
                        for (int j = 0; j < 3; j++)
                            precision += confusionMtx[i][j];
                        precision = confusionMtx[i][i] / precision;

                        double clsLb = (double)(i + 1);

                        System.out.println("\n>>> Precision for class " + clsLb + " is " + precision);

                        double recall = 0.0;
                        for (int j = 0; j < 3; j++)
                            recall += confusionMtx[j][i];
                        recall = confusionMtx[i][i] / recall;
                        System.out.println("\n>>> Recall for class " + clsLb + " is " + recall);

                        double fScore = 2 * precision * recall / (precision + recall);
                        System.out.println("\n>>> F-score for class " + clsLb + " is " + fScore);
                    }

                }
                catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("\n>>> Unexpected exception, check resources: " + e);
                }
                finally {
                    System.out.println("\n>>> kNN classification example completed.");
                }

            });

            igniteThread.start();
            igniteThread.join();
        }
    }
}
