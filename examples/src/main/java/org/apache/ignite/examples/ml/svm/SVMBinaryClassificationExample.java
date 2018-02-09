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

package org.apache.ignite.examples.ml.svm;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledDatasetTestTrainPair;
import org.apache.ignite.ml.structures.preprocessing.LabeledDatasetLoader;
import org.apache.ignite.ml.structures.preprocessing.LabellingMachine;
import org.apache.ignite.ml.structures.preprocessing.Normalizer;
import org.apache.ignite.ml.svm.SVMLinearBinaryClassificationTrainer;
import org.apache.ignite.ml.svm.SVMLinearClassificationModel;
import org.apache.ignite.thread.IgniteThread;

/**
 * <p>
 * Example of using {@link org.apache.ignite.ml.svm.SVMLinearClassificationModel} with Titanic dataset.</p>
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
public class SVMBinaryClassificationExample {
    /** Separator. */
    private static final String SEPARATOR = ",";

    /** Path to the Iris dataset. */
    private static final String TITANIC_DATASET = "examples/src/main/resources/datasets/titanic.txt";

    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     */
    public static void main(String[] args) throws InterruptedException {
        System.out.println(">>> SVM Binary classification example started.");
        // Start ignite grid.
        try (Ignite ignite = Ignition.start("examples/config/example-ignite.xml")) {
            System.out.println(">>> Ignite grid started.");

            IgniteThread igniteThread = new IgniteThread(ignite.configuration().getIgniteInstanceName(),
                SVMBinaryClassificationExample.class.getSimpleName(), () -> {

                try {
                    // Prepare path to read
                    File file = IgniteUtils.resolveIgnitePath(TITANIC_DATASET);
                    if (file == null)
                        throw new RuntimeException("Can't find file: " + TITANIC_DATASET);

                    Path path = file.toPath();

                    // Read dataset from file
                    LabeledDataset dataset = LabeledDatasetLoader.loadFromTxtFile(path, SEPARATOR, true, false);

                    // Normalize dataset
                    Normalizer.normalizeWithMiniMax(dataset);

                    // Random splitting of the given data as 70% train and 30% test datasets
                    LabeledDatasetTestTrainPair split = new LabeledDatasetTestTrainPair(dataset, 0.3);

                    System.out.println("\n>>> Amount of observations in train dataset " + split.train().rowSize());
                    System.out.println("\n>>> Amount of observations in test dataset " + split.test().rowSize());

                    LabeledDataset test = split.test();
                    LabeledDataset train = split.train();

                    System.out.println("\n>>> Create new linear binary SVM trainer object.");
                    Trainer<SVMLinearClassificationModel, LabeledDataset> trainer = new SVMLinearBinaryClassificationTrainer();

                    System.out.println("\n>>> Perform the training to get the model.");
                    SVMLinearClassificationModel mdl = trainer.train(train);

                    System.out.println("\n>>> SVM classification model: " + mdl);

                    // Clone labels
                    final double[] labels = test.labels();

                    // Save predicted classes to test dataset
                    LabellingMachine.assignLabels(test, mdl);

                    // Calculate amount of errors on test dataset
                    int amountOfErrors = 0;
                    for (int i = 0; i < test.rowSize(); i++) {
                        if (test.label(i) != labels[i])
                            amountOfErrors++;
                    }

                    System.out.println("\n>>> Absolute amount of errors " + amountOfErrors);
                    System.out.println("\n>>> Prediction percentage " + (1 - amountOfErrors / (double) test.rowSize()));

                } catch (IOException e) {
                    e.printStackTrace();
                    System.out.println("\n>>> Unexpected exception, check resources: " + e);
                } finally {
                    System.out.println("\n>>> SVM binary classification example completed.");
                }

            });

            igniteThread.start();
            igniteThread.join();
        }
    }
}
