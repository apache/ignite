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

package org.apache.ignite.ml.naivebayes.discrete;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@code DiscreteNaiveBayesModel} */
public class DiscreteNaiveBayesModelTest {
    /** Test. */
    @Test
    public void testPredictWithTwoClasses() {
        double first = 1;
        double second = 2;
        double[][][] probabilities = new double[][][] {
            {{.5, .5}, {.2, .3, .5}, {2. / 3., 1. / 3.}, {.4, .1, .5}, {.5, .5}},
            {{0, 1}, {1. / 7, 2. / 7, 4. / 7}, {4. / 7, 3. / 7}, {2. / 7, 3. / 7, 2. / 7}, {4. / 7, 3. / 7,}}
        };

        double[] classProbabilities = new double[] {6. / 13, 7. / 13};
        double[][] thresholds = new double[][] {{.5}, {.2, .7}, {.5}, {.5, 1.5}, {.5}};
        DiscreteNaiveBayesModel mdl = new DiscreteNaiveBayesModel(probabilities, classProbabilities,
                new double[] {first, second}, thresholds, new DiscreteNaiveBayesSumsHolder());
        Vector observation = VectorUtils.of(2, 0, 1, 2, 0);

        Assert.assertEquals(second, mdl.predict(observation), 0.0001);
    }

}
