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

import org.apache.ignite.ml.TestUtils;
import org.apache.ignite.ml.math.exceptions.CardinalityException;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.regressions.linear.LinearRegressionModel;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link LinearRegressionModel}.
 */
public class SVMModelTest {
    /** Precision in test checks. */
    private static final double PRECISION = 1e-6;

    /** */
    @Test
    public void testPredictWithRawLabels() {
        Vector weights = new DenseVector(new double[]{2.0, 3.0});
        SVMLinearBinaryClassificationModel mdl = new SVMLinearBinaryClassificationModel(weights, 1.0).withRawLabels(true);

        Vector observation = new DenseVector(new double[]{1.0, 1.0});
        TestUtils.assertEquals(1.0 + 2.0 * 1.0 + 3.0 * 1.0, mdl.apply(observation), PRECISION);

        observation = new DenseVector(new double[]{2.0, 1.0});
        TestUtils.assertEquals(1.0 + 2.0 * 2.0 + 3.0 * 1.0, mdl.apply(observation), PRECISION);

        observation = new DenseVector(new double[]{1.0, 2.0});
        TestUtils.assertEquals(1.0 + 2.0 * 1.0 + 3.0 * 2.0, mdl.apply(observation), PRECISION);

        observation = new DenseVector(new double[]{-2.0, 1.0});
        TestUtils.assertEquals(1.0 - 2.0 * 2.0 + 3.0 * 1.0, mdl.apply(observation), PRECISION);

        observation = new DenseVector(new double[]{1.0, -2.0});
        TestUtils.assertEquals(1.0 + 2.0 * 1.0 - 3.0 * 2.0, mdl.apply(observation), PRECISION);

        Assert.assertEquals(true, mdl.isKeepingRawLabels());
    }


    /** */
    @Test
    public void testPredictWithMultiClasses() {
        Vector weights1 = new DenseVector(new double[]{10.0, 0.0});
        Vector weights2 = new DenseVector(new double[]{0.0, 10.0});
        Vector weights3 = new DenseVector(new double[]{-1.0, -1.0});
        SVMLinearMultiClassClassificationModel mdl = new SVMLinearMultiClassClassificationModel();
        mdl.add(1, new SVMLinearBinaryClassificationModel(weights1, 0.0).withRawLabels(true));
        mdl.add(2, new SVMLinearBinaryClassificationModel(weights2, 0.0).withRawLabels(true));
        mdl.add(2, new SVMLinearBinaryClassificationModel(weights3, 0.0).withRawLabels(true));

        Vector observation = new DenseVector(new double[]{1.0, 1.0});
        TestUtils.assertEquals( 1.0, mdl.apply(observation), PRECISION);
    }

    /** */
    @Test
    public void testPredictWithErasedLabels() {
        Vector weights = new DenseVector(new double[]{1.0, 1.0});
        SVMLinearBinaryClassificationModel mdl = new SVMLinearBinaryClassificationModel(weights, 1.0);

        Vector observation = new DenseVector(new double[]{1.0, 1.0});
        TestUtils.assertEquals(1.0, mdl.apply(observation), PRECISION);

        observation = new DenseVector(new double[]{3.0, 4.0});
        TestUtils.assertEquals(1.0, mdl.apply(observation), PRECISION);

        observation = new DenseVector(new double[]{-1.0, -1.0});
        TestUtils.assertEquals(-1.0, mdl.apply(observation), PRECISION);

        observation = new DenseVector(new double[]{-2.0, 1.0});
        TestUtils.assertEquals(-1.0, mdl.apply(observation), PRECISION);

        observation = new DenseVector(new double[]{-1.0, -2.0});
        TestUtils.assertEquals(-1.0, mdl.apply(observation), PRECISION);

        final SVMLinearBinaryClassificationModel mdlWithNewData = mdl.withIntercept(-2.0).withWeights(new DenseVector(new double[] {-2.0, -2.0}));
        System.out.println("The SVM model is " + mdlWithNewData);

        observation = new DenseVector(new double[]{-1.0, -2.0});
        TestUtils.assertEquals(1.0, mdl.apply(observation), PRECISION);
        TestUtils.assertEquals(-2.0, mdl.intercept(), PRECISION);
    }

    /** */
    @Test
    public void testPredictWithErasedLabelsAndChangedThreshold() {
        Vector weights = new DenseVector(new double[]{1.0, 1.0});
        SVMLinearBinaryClassificationModel mdl = new SVMLinearBinaryClassificationModel(weights, 1.0).withThreshold(5);

        Vector observation = new DenseVector(new double[]{1.0, 1.0});
        TestUtils.assertEquals(-1.0, mdl.apply(observation), PRECISION);

        observation = new DenseVector(new double[]{3.0, 4.0});
        TestUtils.assertEquals(1.0, mdl.apply(observation), PRECISION);

        TestUtils.assertEquals(5, mdl.threshold(), PRECISION);
    }

    /** */
    @Test(expected = CardinalityException.class)
    public void testPredictOnAnObservationWithWrongCardinality() {
        Vector weights = new DenseVector(new double[]{2.0, 3.0});

        SVMLinearBinaryClassificationModel mdl = new SVMLinearBinaryClassificationModel(weights, 1.0);

        Vector observation = new DenseVector(new double[]{1.0});

        mdl.apply(observation);
    }
}
