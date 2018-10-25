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

package org.apache.ignite.ml.naivebayes.bernoulli;

import java.io.Serializable;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Simple naive Bayes model which predicts result value {@code y} belongs to a class {@code C_k, k in [0..K]} as {@code
 * p(C_k,y) = p(C_k)*p(y_1,C_k) *...*p(y_n,C_k) / p(y)}. Return the number of the most possible class.
 */
public class BernoulliNaiveBayesModel implements Model<Vector, Double>, Exportable<BernoulliNaiveBayesModel>, Serializable {
    /** */
    private static final long serialVersionUID = -127386523291350345L;
    /** Means of features for all classes. kth row contains means for labels[k] class. */
    private final double[][] probabilities;
    /** Prior probabilities of each class */
    private final double[] classProbabilities;
    /** Labels. */
    private final double[] labels;
    private double binarizeThreshold = .5;
    private double alpha;

    /**
     * @param probabilities Means of features for all classes.
     * @param classProbabilities Probabilities for all classes.
     * @param labels Labels.
     */
    public BernoulliNaiveBayesModel(double[][] probabilities, double[] classProbabilities, double[] labels) {
        this.probabilities = probabilities;
        this.classProbabilities = classProbabilities;
        this.labels = labels;
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<BernoulliNaiveBayesModel, P> exporter, P path) {
        exporter.save(this, path);
    }

    /** Returns a label with max probability. */
    @Override public Double apply(Vector vector) {
        double maxProbapility = .0;
        int max = 0;

        for (int i = 0; i < classProbabilities.length; i++) {
            double p = classProbabilities[i];

            for (int j = 0; j < probabilities.length; j++) {
                int x = toZeroOne(vector.get(j));
                p *= Math.pow(probabilities[i][j], x) * Math.pow(1 - probabilities[i][j], 1 - x);
            }

            if (p > maxProbapility) {
                max = i;
                maxProbapility = p;
            }
        }

        return labels[max];
    }

    /** */
    public double[][] getProbabilities() {
        return probabilities;
    }

    /** */
    public double[] getClassProbabilities() {
        return classProbabilities;
    }

    private int toZeroOne(double value) {
        return value > binarizeThreshold ? 1 : 0;
    }
}
