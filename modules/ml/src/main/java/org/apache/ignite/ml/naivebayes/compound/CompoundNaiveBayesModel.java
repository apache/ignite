package org.apache.ignite.ml.naivebayes.compound;

import java.io.Serializable;
import java.util.Arrays;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.IgniteModel;
import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.naivebayes.discrete.DiscreteNaiveBayesModel;
import org.apache.ignite.ml.naivebayes.gaussian.GaussianNaiveBayesModel;

/** Created by Ravil on 04/02/2019. */
public class CompoundNaiveBayesModel<K, V> implements IgniteModel<Vector, Double>, Exportable<CompoundNaiveBayesModel>, Serializable {

    DiscreteNaiveBayesModel discreteNaiveBayesModel;
    int discreteFeatureFrom;
    int discreteFeatureTo;
    private double[][][] probabilities;
    /** Prior probabilities of each class */
    private double[] clsProbabilities;
    /** Labels. */
    private double[] labels;
    private double[][] bucketThresholds;

    GaussianNaiveBayesModel gaussianNaiveBayesModel;
    int continiousFeatureFrom;
    int continiousFeatureTo;

    /** {@inheritDoc} */
    @Override public <P> void saveModel(Exporter<CompoundNaiveBayesModel, P> exporter, P path) {
        exporter.save(this, path);
    }

    @Override public Double predict(Vector vector) {
        double maxProbapilityPower = -Double.MAX_VALUE;
        double[] probapilityPowers = new double[vector.size()];
        Arrays.fill(probapilityPowers, Double.MAX_VALUE);
        int maxLabelIndex = 0;

        for (int i = 0; i < clsProbabilities.length; i++) {
            probapilityPowers[i] = Math.log(clsProbabilities[i]);
            for (int j = discreteFeatureFrom; j < discreteFeatureTo; j++) {
                int x = toBucketNumber(vector.get(j), bucketThresholds[j]);
                double p = probabilities[i][j][x];
                probapilityPowers[i] += (p > 0 ? Math.log(p) : .0);
            }

            for (int j = continiousFeatureFrom; j < continiousFeatureTo; j++) {
                double x = vector.get(j);
                double g = gauss(x, gaussianNaiveBayesModel.getMeans()[i][j], gaussianNaiveBayesModel.getVariances()[i][j]);
                probapilityPowers[i] += (g > 0 ? Math.log(g) : .0);
            }
        }

        for (int i = 0; i < probapilityPowers.length; i++) {
            if (probapilityPowers[i] > probapilityPowers[maxLabelIndex]) {
                maxLabelIndex = i;
            }
        }
        return labels[maxLabelIndex];
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
}
