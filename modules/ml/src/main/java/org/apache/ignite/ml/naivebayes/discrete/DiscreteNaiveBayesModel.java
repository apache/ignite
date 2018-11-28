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

import java.io.Serializable;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/**
 * Discrete naive Bayes model which predicts result value {@code y} belongs to a class {@code C_k, k in [0..K]} as
 * {@code p(C_k,y) =x_1*p_k1^x *...*x_i*p_ki^x_i}. Where {@code x_i} is a discrete feature, {@code p_ki} is a prior
 * probability probability of class {@code p(x|C_k)}. Returns the number of the most possible class.
 */
public class DiscreteNaiveBayesModel implements Model<Vector, Double>, Exportable<DiscreteNaiveBayesModel>, Serializable {
    /** */
    private static final long serialVersionUID = -127386523291350345L;
    /**
     * Probabilities of features for all classes for each label. {@code labels[c][f][b]} contains a probability for
     * class {@code c} for feature {@code f} for bucket {@code b}.
     */
    private final double[][][] probabilities;
    /** Prior probabilities of each class */
    private final double[] classProbabilities;
    /** Labels. */
    private final double[] labels;
    /** The bucket thresholds to convert a features to discrete values. */
    private final double[][] bucketThresholds;
    /** Amount values in each buckek for each feature per label. */
    private final DiscreteNaiveBayesSumsHolder sumsHolder;

    /**
     * @param probabilities Probabilities of features for classes.
     * @param classProbabilities Prior probabilities for classes.
     * @param bucketThresholds The threshold to convert a feature to a binary value.
     * @param sumsHolder Amount values which are abouve the threshold per label.
     * @param labels Labels.
     */
    public DiscreteNaiveBayesModel(double[][][] probabilities, double[] classProbabilities, double[] labels,
        double[][] bucketThresholds, DiscreteNaiveBayesSumsHolder sumsHolder) {
        this.probabilities = probabilities;
        this.classProbabilities = classProbabilities;
        this.labels = labels;
        this.bucketThresholds = bucketThresholds;
        this.sumsHolder = sumsHolder;
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<DiscreteNaiveBayesModel, P> exporter, P path) {
        exporter.save(this, path);
    }

    /** Returns a label with max probability. */
    @Override public Double apply(Vector vector) {
        double maxProbapilityPower = -Double.MAX_VALUE;
        int maxLabelIndex = -1;

        for (int i = 0; i < classProbabilities.length; i++) {
            double probabilityPower = Math.log(classProbabilities[i]);

            for (int j = 0; j < probabilities[0].length; j++) {
                int x = toBucketNumber(vector.get(j), bucketThresholds[j]);
                double p = probabilities[i][j][x];
                probabilityPower += (p > 0 ? Math.log(p) : .0);
            }

            if (probabilityPower > maxProbapilityPower) {
                maxLabelIndex = i;
                maxProbapilityPower = probabilityPower;
            }
        }
        return labels[maxLabelIndex];
    }

    /** */
    public double[][][] getProbabilities() {
        return probabilities;
    }

    /** */
    public double[] getClassProbabilities() {
        return classProbabilities;
    }

    /** */
    public double[][] getBucketThresholds() {
        return bucketThresholds;
    }

    /** */
    public DiscreteNaiveBayesSumsHolder getSumsHolder() {
        return sumsHolder;
    }

    /** Returs a bucket number to which the {@code value} corresponds. */
    private int toBucketNumber(double value, double[] thresholds) {
        for (int i = 0; i < thresholds.length; i++) {
            if (value < thresholds[i]) {
                return i;
            }
        }
        return thresholds.length;
    }
}
