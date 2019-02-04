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

import org.apache.ignite.ml.math.primitives.matrix.Matrix;
import org.apache.ignite.ml.math.primitives.vector.Vector;

public class MultivariateGaussianDistribution implements Distribution {
    private final Vector mean;
    private final Matrix invCovariance;
    private final double normalizer;

    public MultivariateGaussianDistribution(Vector mean, Matrix covariance) {
        this.mean = mean;
        invCovariance = covariance.inverse();
        normalizer = Math.pow(2 * Math.PI, ((double)invCovariance.rowSize()) / 2) * Math.sqrt(covariance.determinant());
    }

    @Override public double prob(Vector x) {
        Vector delta = x.minus(mean);
        Matrix ePower = delta.toMatrix(true)
            .times(invCovariance)
            .times(delta.toMatrix(false))
            .times(-0.5);
        assert ePower.columnSize() == 1 && ePower.rowSize() == 1;

        return Math.pow(Math.E, ePower.get(0, 0)) / normalizer;
    }
}
