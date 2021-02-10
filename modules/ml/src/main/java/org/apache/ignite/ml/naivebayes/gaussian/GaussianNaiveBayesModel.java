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

import java.util.Collections;
import java.util.List;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.environment.deploy.DeployableObject;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.naivebayes.BayesModel;

/**
 * Simple naive Bayes model which predicts result value {@code y} belongs to a class {@code C_k, k in [0..K]} as {@code
 * p(C_k,y) = p(C_k)*p(y_1,C_k) *...*p(y_n,C_k) / p(y)}. Return the number of the most possible class.
 */
public class GaussianNaiveBayesModel implements BayesModel<GaussianNaiveBayesModel, Vector, Double>, DeployableObject {
    /** Serial version uid. */
    private static final long serialVersionUID = -127386523291350345L;

    /** Means of features for all classes. kth row contains means for labels[k] class. */
    private final double[][] means;

    /** Variances of features for all classes. kth row contains variances for labels[k] class */
    private final double[][] variances;

    /** Prior probabilities of each class */
    private final double[] classProbabilities;

    /** Labels. */
    private final double[] labels;

    /** Feature sum, squared sum and count per label. */
    private final GaussianNaiveBayesSumsHolder sumsHolder;

    /**
     * @param means Means of features for all classes.
     * @param variances Variances of features for all classes.
     * @param classProbabilities Probabilities for all classes.
     * @param labels Labels.
     * @param sumsHolder Feature sum, squared sum and count sum per label. This data is used for future model updating.
     */
    public GaussianNaiveBayesModel(double[][] means, double[][] variances,
        double[] classProbabilities, double[] labels, GaussianNaiveBayesSumsHolder sumsHolder) {
        this.means = means;
        this.variances = variances;
        this.classProbabilities = classProbabilities;
        this.labels = labels;
        this.sumsHolder = sumsHolder;
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<GaussianNaiveBayesModel, P> exporter, P path) {
        exporter.save(this, path);
    }

    /** Returns a number of class to which the input belongs. */
    @Override public Double predict(Vector vector) {
        double[] probabilityPowers = probabilityPowers(vector);

        int max = 0;
        for (int i = 0; i < probabilityPowers.length; i++) {
            probabilityPowers[i] += Math.log(classProbabilities[i]);

            if (probabilityPowers[i] > probabilityPowers[max])
                max = i;
        }
        return labels[max];
    }

    /** {@inheritDoc} */
    @Override public double[] probabilityPowers(Vector vector) {
        double[] probabilityPowers = new double[classProbabilities.length];

        for (int i = 0; i < classProbabilities.length; i++) {
            for (int j = 0; j < vector.size(); j++) {
                double x = vector.get(j);
                double probability = gauss(x, means[i][j], variances[i][j]);
                probabilityPowers[i] += (probability > 0 ? Math.log(probability) : .0);
            }
        }
        return probabilityPowers;
    }

    /** A getter for means.*/
    public double[][] getMeans() {
        return means;
    }

    /** A getter for variances.*/
    public double[][] getVariances() {
        return variances;
    }

    /** A getter for classProbabilities.*/
    public double[] getClassProbabilities() {
        return classProbabilities;
    }

    /** A getter for labels.*/
    public double[] getLabels() {
        return labels;
    }

    /** A getter for sumsHolder.*/
    public GaussianNaiveBayesSumsHolder getSumsHolder() {
        return sumsHolder;
    }

    /** Gauss distribution. */
    private static double gauss(double x, double mean, double variance) {
        return Math.exp(-1. * Math.pow(x - mean, 2) / (2. * variance)) / Math.sqrt(2. * Math.PI * variance);
    }

    /** {@inheritDoc} */
    @Override public List<Object> getDependencies() {
        return Collections.emptyList();
    }
}
