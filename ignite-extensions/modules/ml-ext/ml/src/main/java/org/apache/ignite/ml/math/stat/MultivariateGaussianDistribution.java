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

package org.apache.ignite.ml.math.stat;

import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Distribution represents multidimensional gaussian distribution.
 */
public class MultivariateGaussianDistribution implements Distribution {
    /** Mean. */
    private Vector mean;

    /** Covariance^-1. */
    private Matrix invCovariance;

    /** Normalizer. */
    private double normalizer;

    /** Covariance. */
    private Matrix covariance;

    /**
     * Constructs an instance of MultivariateGaussianDistribution.
     *
     * @param mean Mean.
     * @param covariance Covariance.
     */
    public MultivariateGaussianDistribution(Vector mean, Matrix covariance) {
        A.ensure(covariance.columnSize() == covariance.rowSize(), "Covariance matrix should be square");
        A.ensure(mean.size() == covariance.rowSize(), "Covariance matrix should be built from same space as mean vector");

        this.mean = mean;
        this.covariance = covariance;
        invCovariance = covariance.inverse();

        double determinant = covariance.determinant();
        A.ensure(determinant > 0, "Covariance matrix should be positive definite");
        normalizer = Math.pow(2 * Math.PI, ((double)invCovariance.rowSize()) / 2) * Math.sqrt(determinant);
    }

    /** {@inheritDoc} */
    @Override public double prob(Vector x) {
        Vector delta = x.minus(mean);
        Matrix ePower = delta.toMatrix(true)
            .times(invCovariance)
            .times(delta.toMatrix(false))
            .times(-0.5);
        assert ePower.columnSize() == 1 && ePower.rowSize() == 1;

        return Math.pow(Math.E, ePower.get(0, 0)) / normalizer;
    }

    /** {@inheritDoc} */
    @Override public int dimension() {
        return mean.size();
    }

    /**
     * @return Mean vector.
     */
    public Vector mean() {
        return mean.copy();
    }

    /**
     * @return Covariance matrix.
     */
    public Matrix covariance() {
        return covariance;
    }
}
