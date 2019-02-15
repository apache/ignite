/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.ml.regressions.linear;

import java.io.Serializable;
import java.util.Objects;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Simple linear regression model which predicts result value Y as a linear combination of input variables:
 * Y = weights * X + intercept.
 */
public class LinearRegressionModel implements IgniteModel<Vector, Double>, Exportable<LinearRegressionModel>, Serializable {
    /** */
    private static final long serialVersionUID = -105984600091550226L;

    /** Multiplier of the objects's vector required to make prediction.  */
    private final Vector weights;

    /** Intercept of the linear regression model */
    private final double intercept;

    /** */
    public LinearRegressionModel(Vector weights, double intercept) {
        this.weights = weights;
        this.intercept = intercept;
    }

    /** */
    public Vector getWeights() {
        return weights;
    }

    /** */
    public double getIntercept() {
        return intercept;
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector input) {
        return input.dot(weights) + intercept;
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<LinearRegressionModel, P> exporter, P path) {
        exporter.save(this, path);
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        LinearRegressionModel mdl = (LinearRegressionModel)o;
        return Double.compare(mdl.intercept, intercept) == 0 &&
            Objects.equals(weights, mdl.weights);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {

        return Objects.hash(weights, intercept);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        if (weights.size() < 10) {
            StringBuilder builder = new StringBuilder();

            for (int i = 0; i < weights.size(); i++) {
                double nextItem = i == weights.size() - 1 ? intercept : weights.get(i + 1);

                builder.append(String.format("%.4f", Math.abs(weights.get(i))))
                    .append("*x")
                    .append(i)
                    .append(nextItem > 0 ? " + " : " - ");
            }

            builder.append(String.format("%.4f", Math.abs(intercept)));
            return builder.toString();
        }

        return "LinearRegressionModel [" +
            "weights=" + weights +
            ", intercept=" + intercept +
            ']';
    }

    /** {@inheritDoc} */
    @Override public String toString(boolean pretty) {
        return toString();
    }
}
