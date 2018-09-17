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

package org.apache.ignite.ml.naivebayes.gaussian;

import java.io.Serializable;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Simple naive Bayes model which predicts result value {@code y} belongs to a class {@code C_k, k in [0..K]} as {@code
 * p(C_k,y) = p(C_k)*p(y_1,C_k) *...*p(y_n,C_k) / p(y)}. Return the number of the most possible class.
 */
public class GaussianNaiveBayesModel implements Model<Vector, Integer>, Exportable<GaussianNaiveBayesModel>, Serializable {

    private final double[][] means;
    private final double[][] variances;
    private final Vector classProbabilities;

    /**
     * @param means means of features for all classes
     * @param variances variances of features for all classes
     * @param classProbabilities probabilities for all classes
     */
    public GaussianNaiveBayesModel(double[][] means, double[][] variances,
        Vector classProbabilities) {
        this.means = means;
        this.variances = variances;
        this.classProbabilities = classProbabilities;
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<GaussianNaiveBayesModel, P> exporter, P path) {
        exporter.save(this, path);
    }

    /** Returns a number of class to which the input belongs. */
    @Override public Integer apply(Vector vector) {
        int k = classProbabilities.size();
        double[] probabilites = new double[k];

        for (int i = 0; i < k; i++) {
            double p = classProbabilities.get(i);
            for (int j = 0; j < vector.size(); j++) {
                double x = vector.get(j);
                double g = gauss(x, means[i][j], variances[i][j]);
                p *= g;
            }
            probabilites[i] = p;
        }

        int max = 0;
        for (int i = 0; i < k; i++) {
            if (probabilites[i] > probabilites[max])
                max = i;
        }
        return max;
    }

    private double gauss(double x, double mean, double variance) {
        return Math.exp((-1. * (x - mean) * (x - mean)) / (2. * variance)) / Math.sqrt(2. * Math.PI * variance);
    }
}
