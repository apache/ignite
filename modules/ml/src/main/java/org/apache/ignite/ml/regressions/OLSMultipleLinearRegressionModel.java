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

import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.Vector;

/**
 * Model for linear regression.
 */
public class OLSMultipleLinearRegressionModel implements Model<Vector, Vector>,
    Exportable<OLSMultipleLinearRegressionModelFormat> {
    /** */
    private final AbstractMultipleLinearRegression regression;

    /**
     * Construct linear regression model.
     *
     * @param regression Linear regression object.
     */
    public OLSMultipleLinearRegressionModel(AbstractMultipleLinearRegression regression) {
        this.regression = regression;
    }

    /** {@inheritDoc} */
    @Override public Vector predict(Vector val) {
        regression.newYSampleData(val);

        return regression.getX().times(regression.calculateBeta());
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<OLSMultipleLinearRegressionModelFormat, P> exporter, P path) {
        exporter.save(new OLSMultipleLinearRegressionModelFormat(regression.getX(), regression.isNoIntercept()),
            path);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        OLSMultipleLinearRegressionModel mdl = (OLSMultipleLinearRegressionModel)o;

        return regression.equals(mdl.regression);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return regression.hashCode();
    }
}
