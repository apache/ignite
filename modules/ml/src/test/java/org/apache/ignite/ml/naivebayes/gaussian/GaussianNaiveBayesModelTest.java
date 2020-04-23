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

package org.apache.ignite.ml.naivebayes.gaussian;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link GaussianNaiveBayesModel}.
 */
public class GaussianNaiveBayesModelTest {
    /** Test. */
    @Test
    public void testPredictWithTwoClasses() {
        double first = 1;
        double second = 2;
        double[][] means = new double[][] {
            {5.855, 176.25, 11.25},
            {5.4175, 132.5, 7.5},
        };
        double[][] variances = new double[][] {
            {3.5033E-2, 1.2292E2, 9.1667E-1},
            {9.7225E-2, 5.5833E2, 1.6667},
        };
        double[] probabilities = new double[] {.5, .5};
        GaussianNaiveBayesModel mdl = new GaussianNaiveBayesModel(means, variances, probabilities, new double[] {first, second}, null);
        Vector observation = VectorUtils.of(6, 130, 8);

        Assert.assertEquals(second, mdl.predict(observation), 0.0001);
    }

}
