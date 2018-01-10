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

package org.apache.ignite.ml.svm;

import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.Trainer;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledDatasetTestTrainPair;
import org.apache.ignite.ml.structures.preprocessing.LabellingMachine;

/** Tests behaviour of KNNClassificationTest. */
public class SVMTest extends BaseSVMTest {
    /** */
    public void testPredictOnIrisDataset() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        LabeledDataset dataset = loadDatasetFromTxt(DATASET_PATH, false);

        System.out.println(dataset.rowSize());

        // Random splitting of iris data as 70% train and 30% test datasets
        LabeledDatasetTestTrainPair split = new LabeledDatasetTestTrainPair(dataset, 0.3);

        System.out.println("\n>>> Amount of observations in train dataset " + split.train().rowSize());
        System.out.println("\n>>> Amount of observations in test dataset " + split.test().rowSize());

        LabeledDataset test = split.test();
        LabeledDataset train = split.train();

        System.out.println("\n>>> Create new linear regression trainer object.");
        Trainer<SVMLinearClassificationModel, LabeledDataset> trainer = new SVMLinearClassificationTrainer();

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
        System.out.println("\n>>> Accuracy " + amountOfErrors / (double)test.rowSize());

    }
}
