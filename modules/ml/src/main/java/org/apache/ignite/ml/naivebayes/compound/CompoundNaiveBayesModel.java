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

package org.apache.ignite.ml.naivebayes.compound;

import java.util.function.Predicate;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesModel;

/** Created by Ravil on 04/02/2019. */
public class CompoundNaiveBayesModel implements IgniteModel<Vector, Double>, Exportable<CompoundNaiveBayesModel> {
    /** Prior probabilities of each class. */
    private double[] priorProbabilities;
    /** Labels. */
    private double[] labels;
    /** Gaussian Bayes model. */
    private final GaussianNaiveBayesModel gaussianModel;
    /** Discrete Bayes model. */
    private final DiscreteNaiveBayesModel discreteModel;

    private Predicate<Integer> gaussianSkipFeature;
    private Predicate<Integer> discreteSkipFeature;

    public CompoundNaiveBayesModel(Builder builder) {
        priorProbabilities = builder.priorProbabilities;
        labels = builder.labels;
        gaussianModel = builder.gaussianModel;
        gaussianSkipFeature = builder.gaussianSkipFeature;
        discreteModel = builder.discreteModel;
        discreteSkipFeature = builder.discreteSkipFeature;
    }

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<CompoundNaiveBayesModel, P> exporter, P path) {
        exporter.save(this, path);
    }

    /** {@inheritDoc} */
    @Override public Double predict(Vector vector) {
        double[] probapilityPowers = new double[priorProbabilities.length];
        for (int i = 0; i < priorProbabilities.length; i++) {
            probapilityPowers[i] = Math.log(priorProbabilities[i]);
        }

        if (discreteModel != null) {
            for (int i = 0; i < priorProbabilities.length; i++) {
                for (int j = 0; j < vector.size(); j++) {
                    if (discreteSkipFeature.test(j))
                        continue;
                    int bucketNumber = toBucketNumber(vector.get(j), discreteModel.getBucketThresholds()[j]);
                    double probability = discreteModel.getProbabilities()[i][j][bucketNumber];
                    probapilityPowers[i] += (probability > 0 ? Math.log(probability) : .0);
                }
            }
        }

        if (gaussianModel != null) {
            for (int i = 0; i < priorProbabilities.length; i++) {
                for (int j = 0; j < vector.size(); j++) {
                    if (gaussianSkipFeature.test(j))
                        continue;
                    double parobability = gauss(vector.get(j), gaussianModel.getMeans()[i][j], gaussianModel.getVariances()[i][j]);
                    probapilityPowers[i] += (parobability > 0 ? Math.log(parobability) : .0);
                }
            }
        }
        int maxLabelIndex = 0;
        for (int i = 0; i < probapilityPowers.length; i++) {
            if (probapilityPowers[i] > probapilityPowers[maxLabelIndex]) {
                maxLabelIndex = i;
            }
        }
        return labels[maxLabelIndex];
    }

    public GaussianNaiveBayesModel getGaussianModel() {
        return gaussianModel;
    }

    public DiscreteNaiveBayesModel getDiscreteModel() {
        return discreteModel;
    }

    /** Returs a bucket number to which the {@code value} corresponds. */
    private int toBucketNumber(double val, double[] thresholds) {
        for (int i = 0; i < thresholds.length; i++) {
            if (val < thresholds[i])
                return i;
        }

        return thresholds.length;
    }

    /** Gauss distribution */
    private double gauss(double x, double mean, double variance) {
        return Math.exp(-1. * Math.pow(x - mean, 2) / (2. * variance)) / Math.sqrt(2. * Math.PI * variance);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private double[] priorProbabilities;
        private double[] labels;

        private GaussianNaiveBayesModel gaussianModel;
        private Predicate<Integer> gaussianSkipFeature = i -> false;

        private DiscreteNaiveBayesModel discreteModel;
        private Predicate<Integer> discreteSkipFeature = i -> false;

        public Builder wirhPriorProbabilities(double[] priorProbabilities) {
            this.priorProbabilities = priorProbabilities.clone();
            return this;
        }

        public Builder withLabels(double[] labels) {
            this.labels = labels.clone();
            return this;
        }

        public Builder withGaussianModel(GaussianNaiveBayesModel gaussianModel) {
            this.gaussianModel = gaussianModel;
            return this;
        }

        public Builder withGaussianSkipFuture(Predicate<Integer> gaussianSkipFeature) {
            this.gaussianSkipFeature = gaussianSkipFeature;
            return this;
        }

        public Builder withDiscreteModel(DiscreteNaiveBayesModel discreteModel) {
            this.discreteModel = discreteModel;
            return this;
        }

        public Builder withDiscreteSkipFuture(Predicate<Integer> discreteSkipFeature) {
            this.discreteSkipFeature = discreteSkipFeature;
            return this;
        }

        public CompoundNaiveBayesModel build() {
            return new CompoundNaiveBayesModel(this);
        }
    }
}
