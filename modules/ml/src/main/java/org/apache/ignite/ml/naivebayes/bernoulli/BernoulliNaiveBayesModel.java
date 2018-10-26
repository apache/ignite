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
 * Bernoulli naive Bayes model which predicts result value {@code y} belongs to a class {@code C_k, k in [0..K]} as
 * {@code p(C_k,y) =x_1*p_1^x * (x_1-1)*p_1^(x_1-1)*...*x_i*p_i^x_i+(x_i-1)*p_i^(x_i-1)}. Where {@code x_i} is a binary
 * feature, {@code p_i} is a prior probability probability {@code p(x|C_k)}. Returns the number of the most possible
 * class.
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
    private final double binarizeThreshold;
    private double alpha;
    private final BernoulliNaiveBayesSumsHolder sumsHolder;

    /**
     * @param probabilities Means of features for all classes.
     * @param classProbabilities Probabilities for all classes.
     * @param labels Labels.
     */
    public BernoulliNaiveBayesModel(double[][] probabilities, double[] classProbabilities, double[] labels,
        double binarizeThreshold, BernoulliNaiveBayesSumsHolder sumsHolder) {
        this.probabilities = probabilities;
        this.classProbabilities = classProbabilities;
        this.labels = labels;
        this.binarizeThreshold = binarizeThreshold;
        this.sumsHolder = sumsHolder;
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<BernoulliNaiveBayesModel, P> exporter, P path) {
        exporter.save(this, path);
    }

    /** Returns a label with max probability. */
    @Override public Double apply(Vector vector) {
        double maxProbapility = -Double.MIN_VALUE;
        int max = 0;

        for (int i = 0; i < classProbabilities.length; i++) {
            double probability = Math.log(classProbabilities[i]);

            for (int j = 0; j < probabilities[0].length; j++) {
                int x = toZeroOne(vector.get(j));
                double p = probabilities[i][j];
                probability += (x == 1 ? Math.log(p) : Math.log(1 - p));
            }

            probability = Math.exp(probability);
            if (probability > maxProbapility) {
                max = i;
                maxProbapility = probability;
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

    /** */
    public double getBinarizeThreshold() {
        return binarizeThreshold;
    }

    /** */
    public double getAlpha() {
        return alpha;
    }

    /** */
    public BernoulliNaiveBayesSumsHolder getSumsHolder() {
        return sumsHolder;
    }

    private int toZeroOne(double value) {
        return value > binarizeThreshold ? 1 : 0;
    }
}
