package org.apache.ignite.ml.naivebayes.gaussian;

import java.io.Serializable;
import org.apache.ignite.ml.Exportable;
import org.apache.ignite.ml.Exporter;
import org.apache.ignite.ml.Model;
import org.apache.ignite.ml.math.primitives.vector.Vector;

/** Created by Ravil on 08/09/2018. */
public class GaussianNaiveBayesModel implements Model<Vector, Integer>, Exportable<GaussianNaiveBayesModel>, Serializable {

    private final double[][] means;
    private final double[][] variances;
    private final Vector classProbabilities;

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

    /** returns a vector of probabilities belongins to each class */
    @Override public Integer apply(Vector vector) {
        int k = classProbabilities.size();
        int n = vector.size();
        Double[] probabilites = new Double[k];

        double evidience = 0;
        for (int i = 0; i < k; i++) {
            double p = classProbabilities.get(i);
            for (int j = 0; j < n; j++) {
                double x = vector.get(i);
                double g = gauss(x, means[i][j], variances[i][j]);
                p = p * g;
            }
            probabilites[i] = p;
            evidience += p;
        }
        for (int i = 0; i < k; i++) {
            probabilites[i] /= evidience;
        }
        return 1;
    }

    private double gauss(double x, double mean, double variance) {
        return Math.exp((-1. * (x - mean) * (x - mean)) / (2. * variance)) / Math.sqrt(2. * Math.PI * variance);
    }
}
