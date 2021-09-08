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

package org.apache.ignite.ml.naivebayes.compound;

import java.util.HashMap;
import java.util.Map;

/** Data class which contains test data with precalculated statistics. */
final class Data {
    /** Private constructor. */
    private Data() {
    }

    /** The first label. */
    static final double LABEL_1 = 1.;

    /** The second label. */
    static final double LABEL_2 = 2.;

    /** Labels. */
    static final double[] labels = {LABEL_1, LABEL_2};

    /** */
    static final Map<Integer, double[]> data = new HashMap<>();

    /** Means for gaussian data part. */
    static double[][] means;

    /** Variances for gaussian data part. */
    static double[][] variances;

    /** */
    static double[] classProbabilities;

    /** Thresholds to binarize discrete data. */
    static double[][] binarizedDataThresholds;

    /** Discrete probabilities. */
    static double[][][] probabilities;

    static {
        data.put(0, new double[] {6, 180, 12, 0, 0, 1, 1, 1, LABEL_1});
        data.put(1, new double[] {5.92, 190, 11, 1, 0, 1, 1, 0, LABEL_1});
        data.put(2, new double[] {5.58, 170, 12, 1, 1, 0, 0, 1, LABEL_1});
        data.put(3, new double[] {5.92, 165, 10, 1, 1, 0, 0, 0, LABEL_1});

        data.put(4, new double[] {5, 100, 6, 1, 0, 0, 1, 1, LABEL_2});
        data.put(5, new double[] {5.5, 150, 8, 1, 1, 0, 0, 1, LABEL_2});
        data.put(6, new double[] {5.42, 130, 7, 1, 1, 1, 1, 0, LABEL_2});
        data.put(7, new double[] {5.75, 150, 9, 1, 1, 0, 1, 0, LABEL_2});

        classProbabilities = new double[] {.5, .5};

        means = new double[][] {
            {5.855, 176.25, 11.25},
            {5.4175, 132.5, 7.5},
        };

        variances = new double[][] {
            {3.5033E-2, 1.2292E2, 9.1667E-1},
            {9.7225E-2, 5.5833E2, 1.6667},
        };

        binarizedDataThresholds = new double[][] {{.5}, {.5}, {.5}, {.5}, {.5}};

        probabilities = new double[][][] {
            {{.25, .75}, {.25, .75}, {.5, .5}, {.5, .5}, {.5, .5}},
            {{0, 1}, {.25, .75}, {.75, .25}, {.25, .75}, {.5, .5}}
        };
    }

}
