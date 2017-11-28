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

package org.apache.ignite.ml.knn;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.ml.knn.models.FillMissingValueWith;
import org.apache.ignite.ml.knn.models.KNNModel;
import org.apache.ignite.ml.knn.models.KNNStrategy;
import org.apache.ignite.ml.math.distances.EuclideanDistance;
import org.apache.ignite.ml.structures.LabeledDataset;
import org.apache.ignite.ml.structures.LabeledVector;

/** Tests behaviour of KNNClassificationTest. */
public class KNNClassificationTest2 extends BaseKNNTest {

    public static final String SEPARATOR = "\t";

    // TODO: good idea for example http://www.rpubs.com/Drmadhu/IRISclassification
    // with splitting on test and train data
    public void testCalculateAverageErrorOnIrisDatasetWithSimpleStrategy() {
        IgniteUtils.setCurrentIgniteName(ignite.configuration().getIgniteInstanceName());
        LabeledDataset trainDataset = loadIrisDataset();

        for (int amountOfNeighbours = 1; amountOfNeighbours < 20; amountOfNeighbours += 2) {
            System.out.println("Model initialized with k = " + amountOfNeighbours);

            KNNModel knnModel = new KNNModel(amountOfNeighbours, new EuclideanDistance(), KNNStrategy.SIMPLE, trainDataset);

            int amountOfErrors = 0;
            for (int i = 0; i < trainDataset.rowSize(); i++) {
                LabeledVector row = trainDataset.getRow(i);
                if (row!=null && knnModel.predict(row.features()) != trainDataset.label(i))
                    amountOfErrors++;
            }

            System.out.println("Absolute amount of errors " + amountOfErrors);
            System.out.println("Percentage of errors " + amountOfErrors / (double)trainDataset.rowSize());
        }

    }

    /**
     * Loads labeled dataset from file with .txt extension
     * @return null if path is incorrect
     */
    private LabeledDataset loadIrisDataset() {
        try {
            Path path = Paths.get(this.getClass().getClassLoader().getResource("knn/iris.txt").toURI());
            try {
                return LabeledDataset.loadTxt(path, SEPARATOR, false, false, FillMissingValueWith.ZERO);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }
        catch (URISyntaxException e) {
            e.printStackTrace();
            return null;
        }
        return null;
    }
}
