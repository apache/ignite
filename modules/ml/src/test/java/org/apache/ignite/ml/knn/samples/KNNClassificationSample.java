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

package org.apache.ignite.ml.knn.samples;

import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.knn.BaseKNNTest;
import org.apache.ignite.ml.knn.models.KNNModel;
import org.apache.ignite.ml.knn.models.KNNStrategy;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.structures.LabeledDataset;

/** Tests behaviour of KNNClassificationTest. */
public class KNNClassificationSample extends BaseKNNTest {

    // TODO: good idea for example http://www.rpubs.com/Drmadhu/IRISclassification
    // with splitting on test and train data
    public void testCalculateAverageErrorOnIrisDatasetWithSimpleStrategy() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        LabeledDataset training = loadDatasetFromTxt(KNN_IRIS_TXT, false);

        for (int amountOfNeighbours = 1; amountOfNeighbours < 20; amountOfNeighbours += 2) {
            System.out.println("Model initialized with k = " + amountOfNeighbours);

            KNNModel knnMdl = new KNNModel(amountOfNeighbours, new EuclideanDistance(), KNNStrategy.SIMPLE, training);

            int amountOfErrors = 0;
            for (int i = 0; i < training.rowSize(); i++) {
                if (knnMdl.predict(training.getRow(i).features()) != training.label(i))
                    amountOfErrors++;
            }

            System.out.println("Absolute amount of errors " + amountOfErrors);
            System.out.println("Percentage of errors " + amountOfErrors / (double)training.rowSize());
        }


        // add to sample normalization, column names and etc

    }
}
