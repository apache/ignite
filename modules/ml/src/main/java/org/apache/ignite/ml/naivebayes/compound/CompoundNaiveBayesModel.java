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

import java.util.Collection;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesModel;

import static java.util.Collections.emptyList;

/** Created by Ravil on 04/02/2019. */
public class CompoundNaiveBayesModel implements IgniteModel<Vector, Double>, Exportable<CompoundNaiveBayesModel> {
    /** Prior probabilities of each class. */
    private double[] priorProbabilities;
    /** Labels. */
    private double[] labels;
    /** Gaussian Bayes model. */
    private GaussianNaiveBayesModel gaussianModel;
    /** Discrete Bayes model. */
    private DiscreteNaiveBayesModel discreteModel;

    private Collection<Integer> gaussianSkipFeature = emptyList();
    private Collection<Integer> discreteSkipFeature = emptyList();

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
            probapilityPowers = sum(probapilityPowers,  discreteModel.probabilityPowers(vector));
        }

        if (gaussianModel != null) {
            probapilityPowers = sum(probapilityPowers,  gaussianModel.probabilityPowers(vector));
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

    public CompoundNaiveBayesModel wirhPriorProbabilities(double[] priorProbabilities) {
        this.priorProbabilities = priorProbabilities.clone();
        return this;
    }

    public CompoundNaiveBayesModel withLabels(double[] labels) {
        this.labels = labels.clone();
        return this;
    }

    public CompoundNaiveBayesModel withGaussianModel(GaussianNaiveBayesModel gaussianModel) {
        this.gaussianModel = gaussianModel;
        return this;
    }

    public CompoundNaiveBayesModel withGaussianSkipFuture(Collection<Integer> gaussianSkipFeature) {
        this.gaussianSkipFeature = gaussianSkipFeature;
        return this;
    }

    public CompoundNaiveBayesModel withDiscreteModel(DiscreteNaiveBayesModel discreteModel) {
        this.discreteModel = discreteModel;
        return this;
    }

    public CompoundNaiveBayesModel withDiscreteSkipFuture(Collection<Integer> discreteSkipFeature) {
        this.discreteSkipFeature = discreteSkipFeature;
        return this;
    }

    private static double[] sum(double[] arr1, double[] arr2) {
        assert arr1.length == arr2.length;

        double[] result = new double[arr1.length];

        for (int i = 0; i <arr1.length; i++) {
            result[i] = arr1[i] + arr2[i];
        }
        return result;
    }
}
