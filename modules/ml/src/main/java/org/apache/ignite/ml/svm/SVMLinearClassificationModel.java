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

import java.io.Serializable;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.Vector;

/**
 * Base class for linear classification models.
 */
public class SVMLinearClassificationModel implements Model<Vector, Double>, Exportable<SVMLinearClassificationModel>, Serializable {
    /** Output label format. -1 and +1 for false value and raw distances from the separating hyperplane otherwise.*/
    private boolean isKeepingRawLabels = false;

    /** Threshold to assign +1 label to the observation if raw value more than this threshold.*/
    private double threshold = 0.0;

    // private IgniteFunction regularization = L_one;

    /** Amount of iterations */
    private int amountOfIterations = 20;

    /** Multiplier of the objects's vector required to make prediction.  */
    private final Vector weights;

    /** Intercept of the linear regression model */
    private final double intercept;

    public SVMLinearClassificationModel(Vector weights, double intercept) {
        this.weights = weights;
        this.intercept = intercept;
    }

    /** */
    public SVMLinearClassificationModel withRawLabels(boolean isKeepingRawLabels){
        this.isKeepingRawLabels = isKeepingRawLabels;
        return this;
    }

    /** */
    public SVMLinearClassificationModel withAmountOfIterations(int amountOfIterations){
        this.amountOfIterations = amountOfIterations;
        return this;
    }

    /** {@inheritDoc} */
    @Override public Double apply(Vector vector) {
        return null;
    }

    /**  {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<SVMLinearClassificationModel, P> exporter, P path) {

    }

    public int getAmountOfIterations() {
        return amountOfIterations;
    }

    public boolean isKeepingRawLabels() {
        return isKeepingRawLabels;
    }

    public double getThreshold() {
        return threshold;
    }

    public Vector getWeights() {
        return weights;
    }

    public double getIntercept() {
        return intercept;
    }
}
