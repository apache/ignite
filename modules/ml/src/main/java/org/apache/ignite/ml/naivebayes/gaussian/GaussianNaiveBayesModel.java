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

    /** returns a vector of  belonging probabilities to each class */
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
