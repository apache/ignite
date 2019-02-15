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
package org.apache.ignite.ml.knn.regression;

import java.util.List;
import org.apache.ignite.ml.dataset.Dataset;
import org.apache.ignite.ml.dataset.primitive.context.EmptyContext;
import org.apache.ignite.ml.knn.classification.KNNClassificationModel;
import org.apache.ignite.ml.math.exceptions.UnsupportedOperationException;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.structures.LabeledVector;
import org.apache.ignite.ml.structures.LabeledVectorSet;
import org.apache.ignite.ml.util.ModelTrace;

/**
 * This class provides kNN Multiple Linear Regression or Locally [weighted] regression (Simple and Weighted versions).
 *
 * <p> This is an instance-based learning method. </p>
 *
 * <ul>
 *     <li>Local means using nearby points (i.e. a nearest neighbors approach).</li>
 *     <li>Weighted means we value points based upon how far away they are.</li>
 *     <li>Regression means approximating a function.</li>
 * </ul>
 */
public class KNNRegressionModel extends KNNClassificationModel {
    /** */
    private static final long serialVersionUID = -721836321291120543L;

    /**
     * Builds the model via prepared dataset.
     * @param dataset Specially prepared object to run algorithm over it.
     */
    public KNNRegressionModel(Dataset<EmptyContext, LabeledVectorSet<Double, LabeledVector>> dataset) {
        super(dataset);
    }

    /** {@inheritDoc} */
    @Override public Double apply(Vector v) {
        List<LabeledVector> neighbors = findKNearestNeighbors(v);

        return predictYBasedOn(neighbors, v);
    }

    /** */
    private double predictYBasedOn(List<LabeledVector> neighbors, Vector v) {
        switch (stgy) {
            case SIMPLE:
                return simpleRegression(neighbors);
            case WEIGHTED:
                return weightedRegression(neighbors, v);
            default:
                throw new UnsupportedOperationException("Strategy " + stgy.name() + " is not supported");
        }
    }

    /** */
    private double weightedRegression(List<LabeledVector> neighbors, Vector v) {
        double sum = 0.0;
        double div = 0.0;
        for (LabeledVector<Vector, Double> neighbor : neighbors) {
            double distance = distanceMeasure.compute(v, neighbor.features());
            sum += neighbor.label() * distance;
            div += distance;
        }
        return sum / div;
    }

    /** */
    private double simpleRegression(List<LabeledVector> neighbors) {
        double sum = 0.0;
        for (LabeledVector<Vector, Double> neighbor : neighbors)
            sum += neighbor.label();
        return sum / (double)k;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return toString(false);
    }

    /** {@inheritDoc} */
    @Override public String toString(boolean pretty) {
        return ModelTrace.builder("KNNRegressionModel", pretty)
            .addField("k", String.valueOf(k))
            .addField("measure", distanceMeasure.getClass().getSimpleName())
            .addField("strategy", stgy.name())
            .toString();
    }
}
