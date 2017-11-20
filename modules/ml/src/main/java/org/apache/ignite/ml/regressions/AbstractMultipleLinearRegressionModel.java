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
public class AbstractMultipleLinearRegressionModel implements Model<Vector, Vector>,
    Exportable<AbstractMultipleLinearRegressionModelFormat> {
    /** Root node of the decision tree. */
    private final AbstractMultipleLinearRegression root;

    /**
     * Construct linear regression model.
     *
     * @param root Linear regression object.
     */
    public AbstractMultipleLinearRegressionModel(AbstractMultipleLinearRegression root) {
        this.root = root;
    }

    /** {@inheritDoc} */
    @Override public Vector predict(Vector val) {
        root.newYSampleData(val);

        return root.getX().times(root.calculateBeta());
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<AbstractMultipleLinearRegressionModelFormat, P> exporter, P path) {
        exporter.save(new AbstractMultipleLinearRegressionModelFormat(root.getX(), root.isNoIntercept()), path);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        AbstractMultipleLinearRegressionModel mdl = (AbstractMultipleLinearRegressionModel)o;

        return root.equals(mdl.root);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return root.hashCode();
    }
}
