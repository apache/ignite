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

package org.apache.ignite.ml.regressions;

import org.apache.ignite.ml.math.Matrix;
import org.apache.ignite.ml.math.Vector;
import org.apache.ignite.ml.math.exceptions.MathIllegalArgumentException;
import org.apache.ignite.ml.math.exceptions.NullArgumentException;
import org.apache.ignite.ml.math.impls.matrix.DenseLocalOnHeapMatrix;
import org.apache.ignite.ml.math.impls.vector.DenseLocalOnHeapVector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * This class is based on the corresponding class from Apache Common Math lib.
 * Abstract base class for implementations of {@link MultipleLinearRegression}.
 */
public abstract class AbstractMultipleLinearRegressionTest {
    /** */
    protected AbstractMultipleLinearRegression regression;

    /** */
    @Before
    public void setUp() {
        regression = createRegression();
    }

    /** */
    protected abstract AbstractMultipleLinearRegression createRegression();

    /** */
    protected abstract int getNumberOfRegressors();

    /** */
    protected abstract int getSampleSize();

    /** */
    @Test
    public void canEstimateRegressionParameters() {
        double[] beta = regression.estimateRegressionParameters();
        Assert.assertEquals(getNumberOfRegressors(), beta.length);
    }

    /** */
    @Test
    public void canEstimateResiduals() {
        double[] e = regression.estimateResiduals();
        Assert.assertEquals(getSampleSize(), e.length);
    }

    /** */
    @Test
    public void canEstimateRegressionParametersVariance() {
        Matrix var = regression.estimateRegressionParametersVariance();
        Assert.assertEquals(getNumberOfRegressors(), var.rowSize());
    }

    /** */
    @Test
    public void canEstimateRegressandVariance() {
        if (getSampleSize() > getNumberOfRegressors()) {
            double variance = regression.estimateRegressandVariance();
            Assert.assertTrue(variance > 0.0);
        }
    }

    /**
     * Verifies that newSampleData methods consistently insert unitary columns
     * in design matrix.  Confirms the fix for MATH-411.
     */
    @Test
    public void testNewSample() {
        double[] design = new double[] {
            1, 19, 22, 33,
            2, 20, 30, 40,
            3, 25, 35, 45,
            4, 27, 37, 47
        };

        double[] y = new double[] {1, 2, 3, 4};

        double[][] x = new double[][] {
            {19, 22, 33},
            {20, 30, 40},
            {25, 35, 45},
            {27, 37, 47}
        };

        AbstractMultipleLinearRegression regression = createRegression();
        regression.newSampleData(design, 4, 3, new DenseLocalOnHeapMatrix());

        Matrix flatX = regression.getX().copy();
        Vector flatY = regression.getY().copy();

        regression.newXSampleData(new DenseLocalOnHeapMatrix(x));
        regression.newYSampleData(new DenseLocalOnHeapVector(y));

        Assert.assertEquals(flatX, regression.getX());
        Assert.assertEquals(flatY, regression.getY());

        // No intercept
        regression.setNoIntercept(true);
        regression.newSampleData(design, 4, 3, new DenseLocalOnHeapMatrix());

        flatX = regression.getX().copy();
        flatY = regression.getY().copy();

        regression.newXSampleData(new DenseLocalOnHeapMatrix(x));
        regression.newYSampleData(new DenseLocalOnHeapVector(y));

        Assert.assertEquals(flatX, regression.getX());
        Assert.assertEquals(flatY, regression.getY());
    }

    /** */
    @Test(expected = NullArgumentException.class)
    public void testNewSampleNullData() {
        double[] data = null;
        createRegression().newSampleData(data, 2, 3, new DenseLocalOnHeapMatrix());
    }

    /** */
    @Test(expected = MathIllegalArgumentException.class)
    public void testNewSampleInvalidData() {
        double[] data = new double[] {1, 2, 3, 4};
        createRegression().newSampleData(data, 2, 3, new DenseLocalOnHeapMatrix());
    }

    /** */
    @Test(expected = MathIllegalArgumentException.class)
    public void testNewSampleInsufficientData() {
        double[] data = new double[] {1, 2, 3, 4};
        createRegression().newSampleData(data, 1, 3, new DenseLocalOnHeapMatrix());
    }

    /** */
    @Test(expected = NullArgumentException.class)
    public void testXSampleDataNull() {
        createRegression().newXSampleData(null);
    }

    /** */
    @Test(expected = NullArgumentException.class)
    public void testYSampleDataNull() {
        createRegression().newYSampleData(null);
    }

}
