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

package org.apache.ignite.examples.ml.util;

import java.util.Arrays;
import org.apache.ignite.ml.dataset.primitive.SimpleDataset;

/**
 * Common utility code used in some ML examples to report some statistic metrics of the dataset.
 */
public class DatasetHelper {
    /**
     *
     */
    private final SimpleDataset dataset;

    /**
     *
     */
    public DatasetHelper(SimpleDataset dataset) {
        this.dataset = dataset;
    }

    /**
     *
     */
    public void describe() {
        // Calculation of the mean value. This calculation will be performed in map-reduce manner.
        double[] mean = dataset.mean();
        System.out.println("Mean \n\t" + Arrays.toString(mean));

        // Calculation of the standard deviation. This calculation will be performed in map-reduce manner.
        double[] std = dataset.std();
        System.out.println("Standard deviation \n\t" + Arrays.toString(std));

        // Calculation of the covariance matrix.  This calculation will be performed in map-reduce manner.
        double[][] cov = dataset.cov();
        System.out.println("Covariance matrix ");
        for (double[] row : cov)
            System.out.println("\t" + Arrays.toString(row));

        // Calculation of the correlation matrix. This calculation will be performed in map-reduce manner.
        double[][] corr = dataset.corr();
        System.out.println("Correlation matrix ");
        for (double[] row : corr)
            System.out.println("\t" + Arrays.toString(row));

        System.out.flush();
    }
}
