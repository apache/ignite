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

package org.apache.ignite.ml.regressions.linear;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.Vector;

/**
 * Simple linear regression model which predicts result Y as a linear combination of input variables: Y = b * X.
 */
public class LinearRegressionModel implements Model<Vector, Double>, Exportable<LinearRegressionModel>, Serializable {
    /** */
    private static final long serialVersionUID = -105984600091550226L;

    /** */
    private final Vector weights;

    /** */
    private final double intercept;

    /** */
    public LinearRegressionModel(Vector weights, double intercept) {
        this.weights = weights;
        this.intercept = intercept;
    }

    /**
     * {@inheritDoc}
     */
    @Override public Double apply(Vector input) {
        return input.dot(weights) + intercept;
    }

    /** */
    public Vector getWeights() {
        return weights;
    }

    /** */
    public double getIntercept() {
        return intercept;
    }

    /**
     * {@inheritDoc}
     */
    @Override public <P> void saveModel(Exporter<LinearRegressionModel, P> exporter, P path) {
        exporter.save(this, path);
    }

    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        LinearRegressionModel model = (LinearRegressionModel)o;
        return Double.compare(model.intercept, intercept) == 0 &&
            Objects.equals(weights, model.weights);
    }

    @Override public int hashCode() {

        return Objects.hash(weights, intercept);
    }
}