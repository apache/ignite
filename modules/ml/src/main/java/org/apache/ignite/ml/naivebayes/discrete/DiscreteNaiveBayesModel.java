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

package org.apache.ignite.ml.naivebayes.discrete;

import java.util.Collections;
import java.util.List;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.environment.deploy.DeployableObject;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.naivebayes.BayesModel;

/**
 * Discrete naive Bayes model which predicts result value {@code y} belongs to a class {@code C_k, k in [0..K]} as
 * {@code p(C_k,y) =x_1*p_k1^x *...*x_i*p_ki^x_i}. Where {@code x_i} is a discrete feature, {@code p_ki} is a prior
 * probability probability of class {@code p(x|C_k)}. Returns the number of the most possible class.
 */
public class DiscreteNaiveBayesModel implements BayesModel<DiscreteNaiveBayesModel, Vector, Double>, DeployableObject {
    /** Serial version uid. */
    private static final long serialVersionUID = -127386523291350345L;

    /**
     * Probabilities of features for all classes for each label. {@code labels[c][f][b]} contains a probability for
     * class {@code c} for feature {@code f} for bucket {@code b}.
     */
    private final double[][][] probabilities;

    /** Prior probabilities of each class */
    private final double[] clsProbabilities;

    /** Labels. */
    private final double[] labels;

    /**
     * The bucket thresholds to convert a features to discrete values. {@code bucketThresholds[f][b]} contains the right
     * border for feature {@code f} for bucket {@code b}. Everything which is above the last thresdold goes to the next
     * bucket.
     */
    private final double[][] bucketThresholds;

    /** Amount values in each buckek for each feature per label. */
    private final DiscreteNaiveBayesSumsHolder sumsHolder;

    /**
     * @param probabilities Probabilities of features for classes.
     * @param clsProbabilities Prior probabilities for classes.
     * @param labels Labels.
     * @param bucketThresholds The threshold to convert a feature to a binary value.
     * @param sumsHolder Amount values which are abouve the threshold per label.
     */
    public DiscreteNaiveBayesModel(double[][][] probabilities, double[] clsProbabilities, double[] labels,
        double[][] bucketThresholds, DiscreteNaiveBayesSumsHolder sumsHolder) {
        this.probabilities = probabilities;
        this.clsProbabilities = clsProbabilities;
        this.labels = labels;
        this.bucketThresholds = bucketThresholds;
        this.sumsHolder = sumsHolder;
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<DiscreteNaiveBayesModel, P> exporter, P path) {
        exporter.save(this, path);
    }

    /**
     * @param vector features vector.
     * @return A label with max probability.
     */
    @Override public Double predict(Vector vector) {
        double[] probapilityPowers = probabilityPowers(vector);

        int maxLbIdx = 0;
        for (int i = 0; i < probapilityPowers.length; i++) {
            probapilityPowers[i] += Math.log(clsProbabilities[i]);

            if (probapilityPowers[i] > probapilityPowers[maxLbIdx])
                maxLbIdx = i;
        }

        return labels[maxLbIdx];
    }

    /** {@inheritDoc} */
    @Override public double[] probabilityPowers(Vector vector) {
        double[] probapilityPowers = new double[clsProbabilities.length];

        for (int i = 0; i < clsProbabilities.length; i++) {
            for (int j = 0; j < probabilities[0].length; j++) {
                int x = toBucketNumber(vector.get(j), bucketThresholds[j]);
                double p = probabilities[i][j][x];
                probapilityPowers[i] += (p > 0 ? Math.log(p) : .0);
            }
        }

        return probapilityPowers;
    }

    /** A getter for probabilities.*/
    public double[][][] getProbabilities() {
        return probabilities;
    }

    /** A getter for clsProbabilities.*/
    public double[] getClsProbabilities() {
        return clsProbabilities;
    }

    /** A getter for bucketThresholds.*/
    public double[][] getBucketThresholds() {
        return bucketThresholds;
    }

    /** A getter for labels.*/
    public double[] getLabels() {
        return labels;
    }

    /** A getter for sumsHolder.*/
    public DiscreteNaiveBayesSumsHolder getSumsHolder() {
        return sumsHolder;
    }

    /** Returs a bucket number to which the {@code value} corresponds. */
    private int toBucketNumber(double val, double[] thresholds) {
        for (int i = 0; i < thresholds.length; i++) {
            if (val < thresholds[i])
                return i;
        }

        return thresholds.length;
    }

    /** {@inheritDoc} */
    @Override public List<Object> getDependencies() {
        return Collections.emptyList();
    }
}
