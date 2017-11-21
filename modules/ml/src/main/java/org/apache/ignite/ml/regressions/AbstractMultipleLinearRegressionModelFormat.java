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

import java.io.Serializable;
import org.apache.ignite.ml.math.Matrix;

/**
 * Linear regression model representation.
 *
 * @see OLSMultipleLinearRegressionModel
 */
public class AbstractMultipleLinearRegressionModelFormat implements Serializable {
    /** X sample data. */
    private final Matrix xMatrix;

    /** Whether or not the regression model includes an intercept.  True means no intercept. */
    private final boolean noIntercept;

    /** */
    public AbstractMultipleLinearRegressionModelFormat(Matrix xMatrix, boolean noIntercept) {
        this.xMatrix = xMatrix;
        this.noIntercept = noIntercept;
    }

    /** */
    public OLSMultipleLinearRegressionModel getAbstractMultipleLinearRegressionModel() {
        OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
        regression.setNoIntercept(true);
        regression.newXSampleData(xMatrix);
        regression.setNoIntercept(noIntercept);

        return new OLSMultipleLinearRegressionModel(regression);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        AbstractMultipleLinearRegressionModelFormat format = (AbstractMultipleLinearRegressionModelFormat)o;

        return noIntercept == format.noIntercept && xMatrix.equals(format.xMatrix);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int res = xMatrix.hashCode();
        res = 31 * res + (noIntercept ? 1 : 0);
        return res;
    }
}
