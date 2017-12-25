package org.apache.ignite.ml.optimization;

import org.apache.ignite.ml.math.Vector;

/**
 * Updater based in Barzilai-Borwein method which guarantees convergence.
 */
public class BarzilaiBorweinUpdater implements Updater {

    /** */
    private static final long serialVersionUID = 5046575099408708472L;

    /**
     * Learning rate used on the first iteration.
     */
    private static final double INITIAL_LEARNING_RATE = 1.0;

    /**
     * {@inheritDoc}
     */
    @Override
    public Vector compute(Vector oldWeights, Vector oldGradient, Vector weights, Vector gradient, int iteration) {
        double learningRate = computeLearningRate(oldWeights, oldGradient, weights, gradient);
        return weights.minus(gradient.times(learningRate));
    }

    /** */
    private double computeLearningRate(Vector oldWeights, Vector oldGradient, Vector weights, Vector gradient) {
        if (oldWeights == null || oldGradient == null) {
            return INITIAL_LEARNING_RATE;
        }
        else {
            Vector gradientDiff = gradient.minus(oldGradient);
            return weights.minus(oldWeights).dot(gradientDiff) / Math.pow(gradientDiff.kNorm(2.0), 2.0);
        }
    }
}
