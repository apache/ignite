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

package org.apache.ignite.ml.naivebayes.bernoulli;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.VectorUtils;
import org.junit.Assert;
import org.junit.Test;

/** Tests for {@code BernoulliNaiveBayesModel} */
public class BernoulliNaiveBayesModelTest {
    /** */
    @Test
    public void testPredictWithTwoClasses() {
        double first = 1;
        double second = 2;
        double[][] probabilities = new double[][] {
            {.5, .5, 1. / 3., .5, .5},
            {1, 4. / 7, 3. / 7, 5. / 7, 3. / 7}
        };

        double[] classProbabilities = new double[] {6. / 13, 7. / 13};
        BernoulliNaiveBayesModel mdl = new BernoulliNaiveBayesModel(probabilities, classProbabilities, new double[] {first, second}, .5, new BernoulliNaiveBayesSumsHolder());
        Vector observation = VectorUtils.of(1, 0, 1, 1, 0);

        Assert.assertEquals(second, mdl.apply(observation), 0.0001);
    }

}
